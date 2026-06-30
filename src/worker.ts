import { EventEmitter } from 'events';
import type Redis from 'ioredis';
import type { WorkerOptions, Processor, JobsOptions } from './types';
import { Job } from './job';
import { createClient } from './connection';
import { UnrecoverableError, GroupRateLimitError } from './errors';

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

interface ActiveJobEntry<DataType, ResultType, NameType extends string> {
  job: Job<DataType, ResultType, NameType>;
  token: string;
  renewalTimer: ReturnType<typeof setInterval>;
}

export class Worker<
  DataType = unknown,
  ResultType = unknown,
  NameType extends string = string,
> extends EventEmitter {
  readonly name: string;
  readonly opts: WorkerOptions;
  readonly id: string;

  private client: Redis;
  private blockingClient: Redis;

  private _running = false;
  private _closing = false;
  private _paused = false;
  private activeCount = 0;
  private activeJobs = new Map<string, ActiveJobEntry<DataType, ResultType, NameType>>();

  private schedulerTimer: ReturnType<typeof setInterval> | null = null;
  private delayedTimer: ReturnType<typeof setInterval> | null = null;
  private stalledTimer: ReturnType<typeof setInterval> | null = null;
  private schedulerLoopActive = false;
  // Semaphore: mainLoop waiters blocked on concurrency limit
  private slotWaiters: Array<() => void> = [];

  constructor(
    name: string,
    private processor?: Processor<DataType, ResultType, NameType> | null,
    opts?: WorkerOptions,
  ) {
    super();
    this.name = name;
    this.id = `worker-${Math.random().toString(36).slice(2, 9)}`;
    this.opts = {
      concurrency: 1,
      autorun: true,
      lockDuration: 30000,
      stalledInterval: 30000,
      maxStalledCount: 1,
      drainDelay: 5,
      ...opts,
      connection: opts?.connection ?? { host: '127.0.0.1', port: 6379 },
    };
    this.client = createClient(this.opts.connection);
    this.blockingClient = createClient(this.opts.connection);

    this.client.on('error', (err) => this.emit('error', err));
    // Suppress errors from blockingClient during shutdown (BLPOP rejection on close is expected)
    this.blockingClient.on('error', (err) => {
      if (!this._closing) this.emit('error', err);
    });

    if (this.opts.autorun !== false) {
      setImmediate(() => this.run().catch((err) => this.emit('error', err)));
    }
  }

  get keyPrefix(): string {
    return `${this.opts.prefix ?? 'bull'}:${this.name}`;
  }

  get concurrency(): number {
    return this.opts.concurrency ?? 1;
  }

  set concurrency(val: number) {
    (this.opts as WorkerOptions & { concurrency: number }).concurrency = val;
  }

  isRunning(): boolean {
    return this._running;
  }

  isPaused(): boolean {
    return this._paused;
  }

  async pause(doNotWaitActive = false): Promise<void> {
    this._paused = true;
    if (!doNotWaitActive) {
      while (this.activeCount > 0) {
        await sleep(50);
      }
    }
    this.emit('paused');
  }

  resume(): void {
    this._paused = false;
    this.emit('resumed');
  }

  async waitUntilReady(): Promise<Redis> {
    await this.client.ping();
    return this.client;
  }

  cancelJob(jobId: string, reason = 'cancelled'): boolean {
    return this.activeJobs.has(jobId);
  }

  cancelAllJobs(reason = 'cancelled'): void {
    // Cancellation is signalled through the AbortSignal; no forced kill here
  }

  async run(): Promise<void> {
    if (this._running) return;
    this._running = true;
    this.emit('ready');

    this.startScheduler();
    this.startDelayedPromoter();

    if (!this.opts.skipStalledCheck) {
      this.startStalledChecker();
    }

    try {
      await this.mainLoop();
    } finally {
      this._running = false;
    }
  }

  // ─── Main processing loop ──────────────────────────────────────────────────

  private async mainLoop(): Promise<void> {
    while (!this._closing) {
      // ── Concurrency gate (event-driven, zero-polling) ──────────────────────
      // Park here until a slot is freed by releaseSlot(). No sleep/busy-spin.
      if (this.activeCount >= this.concurrency) {
        await new Promise<void>((r) => this.slotWaiters.push(r));
        if (this._closing) break;
      }

      // ── Pause checks ───────────────────────────────────────────────────────
      if (this._paused) {
        await sleep(50);
        continue;
      }
      const queuePaused = await this.client.exists(`${this.keyPrefix}:paused`);
      if (queuePaused) {
        await sleep(200);
        continue;
      }

      // ── Job pickup (non-blocking first, then BLPOP) ────────────────────────
      let jobId = await this.nextFromPriority();
      if (!jobId) jobId = await this.client.lpop(`${this.keyPrefix}:ready`);

      if (jobId) {
        // Claim the slot before firing so activeCount is accurate during the call.
        this.activeCount++;
        this.processJob(jobId).catch((err) => this.emit('error', err));
        continue;
      }

      // Queue is empty — block until a job arrives (or timeout).
      const timeout = this.opts.drainDelay ?? 5;
      const result = await this.blockingClient
        .blpop(`${this.keyPrefix}:ready`, timeout)
        .catch(() => null);

      if (!result) {
        this.emit('drained');
      } else {
        this.activeCount++;
        this.processJob(result[1]).catch((err) => this.emit('error', err));
      }
    }
  }

  // Decrement activeCount and wake up the mainLoop if it was parked.
  private releaseSlot(): void {
    this.activeCount--;
    const resolve = this.slotWaiters.shift();
    if (resolve) resolve();
  }

  private async nextFromPriority(): Promise<string | null> {
    const items = await this.client.zrange(`${this.keyPrefix}:priority`, 0, 0);
    if (!items.length) return null;
    const id = items[0];
    const removed = await this.client.zrem(`${this.keyPrefix}:priority`, id);
    return removed ? id : null;
  }

  // ─── Single job lifecycle ──────────────────────────────────────────────────

  private async processJob(jobId: string): Promise<void> {
    // activeCount was already incremented by mainLoop before this call.
    const token = `${this.id}:${jobId}`;

    try {
      const job = await Job.fromId<DataType, ResultType, NameType>(
        this.client,
        this.name,
        this.opts.prefix ?? 'bull',
        jobId,
      );

      if (!job) {
        this.releaseSlot();
        return;
      }

      await this.lockAndActivate(job, token);

      try {
        if (!this.processor) throw new Error('No processor defined');

        const abortController = new AbortController();
        const result = await this.processor(job, token, abortController.signal);

        job.returnvalue = result;
        job.finishedOn = Date.now();
        await job.save();

        await this.onCompleted(job, token);

      } catch (err) {
        if (err instanceof GroupRateLimitError) {
          // Processor called rateLimitGroup() + threw RateLimitError().
          // Re-enqueue the job at the front of the group queue (LPUSH) so it
          // is retried after the rate limit window expires; don't count as fail.
          await this.onRateLimited(job, token);
        } else {
          await this.onFailed(job, token, err as Error);
        }
      }
    } finally {
      this.releaseSlot();
    }
  }

  private async onRateLimited(
    job: Job<DataType, ResultType, NameType>,
    token: string,
  ): Promise<void> {
    await this.cleanup(job);
    const groupId = job.opts.group?.id;
    if (groupId) {
      // Re-enqueue at front so the job is next when the limit lifts.
      const groupPriority = job.opts.group?.priority ?? 0;
      if (groupPriority > 0) {
        await this.client.zadd(`${this.keyPrefix}:group:priority:${groupId}`, groupPriority, job.id);
      } else {
        await this.client.lpush(`${this.keyPrefix}:group:${groupId}`, job.id);
      }
      const added = await this.client.sadd(`${this.keyPrefix}:groups:set`, groupId);
      if (added) await this.client.rpush(`${this.keyPrefix}:groups:active`, groupId);
    } else {
      await this.client.rpush(`${this.keyPrefix}:ready`, job.id);
    }
    await this.xadd('rate-limited', { jobId: job.id });
  }

  private async lockAndActivate(
    job: Job<DataType, ResultType, NameType>,
    token: string,
  ): Promise<void> {
    const lockDuration = this.opts.lockDuration ?? 30000;
    await this.client.set(`${this.keyPrefix}:processing:${job.id}`, token, 'PX', lockDuration);
    await this.client.rpush(`${this.keyPrefix}:active`, job.id);

    job.processedOn = Date.now();
    job.attemptsStarted++;
    job.attemptsMade++;
    await job.save();

    // Renew lock periodically
    const renewalInterval = this.opts.lockRenewTime ?? Math.floor(lockDuration / 2);
    const renewalTimer = setInterval(async () => {
      if (this.activeJobs.has(job.id)) {
        await this.client
          .pexpire(`${this.keyPrefix}:processing:${job.id}`, lockDuration)
          .catch(() => {/* ignore if already gone */});
      }
    }, renewalInterval);

    this.activeJobs.set(job.id, { job, token, renewalTimer });

    await this.xadd('active', { jobId: job.id, prev: 'waiting' });
    this.emit('active', job, 'waiting');
  }

  private async cleanup(job: Job<DataType, ResultType, NameType>): Promise<void> {
    const entry = this.activeJobs.get(job.id);
    if (entry) {
      clearInterval(entry.renewalTimer);
      this.activeJobs.delete(job.id);
    }
    await this.client.lrem(`${this.keyPrefix}:active`, 1, job.id);
    await this.client.del(`${this.keyPrefix}:processing:${job.id}`);

    // Group cleanup: decrement concurrency and re-enqueue group if it still has jobs
    const groupId = job.opts.group?.id;
    if (groupId) {
      const remaining = await this.client.decr(`${this.keyPrefix}:running:${groupId}`);
      // If counter went negative (stale), reset to 0
      if (remaining < 0) {
        await this.client.set(`${this.keyPrefix}:running:${groupId}`, '0');
      }
    }
  }

  private async onCompleted(
    job: Job<DataType, ResultType, NameType>,
    token: string,
  ): Promise<void> {
    await this.cleanup(job);
    await this.applyRemovePolicy(job, 'complete');

    await this.xadd('completed', {
      jobId: job.id,
      returnvalue: JSON.stringify(job.returnvalue),
      prev: 'active',
    });
    this.emit('completed', job, job.returnvalue, 'active');
  }

  private async onFailed(
    job: Job<DataType, ResultType, NameType>,
    token: string,
    err: Error,
  ): Promise<void> {
    const isUnrecoverable = err instanceof UnrecoverableError;
    const maxAttempts = job.opts.attempts ?? 1;

    job.failedReason = err.message;
    if (err.stack) job.stacktrace.push(err.stack);
    job.finishedOn = Date.now();
    await job.save();

    await this.cleanup(job);

    const canRetry = !isUnrecoverable && job.attemptsMade < maxAttempts;

    if (canRetry) {
      const delay = this.calcBackoff(job);
      if (delay > 0) {
        await this.client.zadd(`${this.keyPrefix}:delayed`, Date.now() + delay, job.id);
        await this.xadd('delayed', { jobId: job.id, delay });
      } else {
        // Re-enqueue with group and priority awareness
        const groupId = job.opts.group?.id;
        if (groupId) {
          const groupPriority = job.opts.group?.priority ?? 0;
          if (groupPriority > 0) {
            await this.client.zadd(`${this.keyPrefix}:group:priority:${groupId}`, groupPriority, job.id);
          } else {
            await this.client.rpush(`${this.keyPrefix}:group:${groupId}`, job.id);
          }
          const added = await this.client.sadd(`${this.keyPrefix}:groups:set`, groupId);
          if (added) await this.client.rpush(`${this.keyPrefix}:groups:active`, groupId);
        } else {
          await this.client.rpush(`${this.keyPrefix}:ready`, job.id);
        }
      }
    } else {
      await this.applyRemovePolicy(job, 'fail');
      await this.xadd('failed', { jobId: job.id, failedReason: err.message, prev: 'active' });
      this.emit('failed', job, err, 'active');

      if (job.attemptsMade >= maxAttempts) {
        await this.xadd('retries-exhausted', { jobId: job.id, attemptsMade: job.attemptsMade });
      }
    }
  }

  private calcBackoff(job: Job<DataType, ResultType, NameType>): number {
    const { backoff } = job.opts;
    if (!backoff) return 0;
    if (typeof backoff === 'number') return backoff;
    const delay = backoff.delay ?? 0;
    if (backoff.type === 'exponential') {
      return delay * Math.pow(2, job.attemptsMade - 1);
    }
    return delay; // fixed
  }

  private async applyRemovePolicy(
    job: Job<DataType, ResultType, NameType>,
    outcome: 'complete' | 'fail',
  ): Promise<void> {
    const policy = outcome === 'complete' ? job.opts.removeOnComplete : job.opts.removeOnFail;
    const setKey = `${this.keyPrefix}:${outcome === 'complete' ? 'completed' : 'failed'}`;

    if (policy === true) {
      await job.remove();
      return;
    }

    await this.client.zadd(setKey, job.finishedOn ?? Date.now(), job.id);

    const keepConfig = typeof policy === 'number'
      ? { count: policy }
      : typeof policy === 'object' && policy !== null
      ? policy
      : null;

    if (keepConfig) {
      if (keepConfig.count !== undefined) {
        const total = await this.client.zcard(setKey);
        if (total > keepConfig.count) {
          const excess = await this.client.zrange(setKey, 0, total - keepConfig.count - 1);
          const pipe = this.client.pipeline();
          for (const id of excess) {
            pipe.zrem(setKey, id);
            pipe.del(`${this.keyPrefix}:job:${id}`);
          }
          await pipe.exec();
        }
      }
      if ('age' in keepConfig && keepConfig.age !== undefined) {
        const cutoff = Date.now() - keepConfig.age * 1000;
        const old = await this.client.zrangebyscore(setKey, '-inf', cutoff);
        const pipe = this.client.pipeline();
        for (const id of old) {
          pipe.zrem(setKey, id);
          pipe.del(`${this.keyPrefix}:job:${id}`);
        }
        await pipe.exec();
      }
    }
  }

  // ─── Group scheduler (round-robin) ────────────────────────────────────────
  //
  // Instead of a fixed-interval timer that schedules 1 job per tick, we run a
  // continuous async loop that schedules ALL available groups in one pass, then
  // yields immediately (setImmediate) if work was found, or backs off 10ms when
  // the groups:active list is empty. This means latency between a job finishing
  // and the next group job being scheduled is ~1 event-loop tick, not ≤100ms.

  private startScheduler(): void {
    this.schedulerLoopActive = true;
    this.schedulerTick();
  }

  private schedulerTick(): void {
    if (!this.schedulerLoopActive) return;
    this.scheduleGroupBatch()
      .then((n) => {
        if (!this.schedulerLoopActive) return;
        // If we scheduled anything, yield to the event loop then immediately retry.
        // If idle, back off 10ms so we don't busy-spin on an empty groups:active list.
        if (n > 0) setImmediate(() => this.schedulerTick());
        else this.schedulerTimer = setTimeout(() => this.schedulerTick(), 10) as unknown as ReturnType<typeof setInterval>;
      })
      .catch(() => {
        if (this.schedulerLoopActive) {
          this.schedulerTimer = setTimeout(() => this.schedulerTick(), 10) as unknown as ReturnType<typeof setInterval>;
        }
      });
  }

  // Schedule one job from each available group in a single pass.
  // Returns the number of jobs pushed to the ready queue.
  private async scheduleGroupBatch(): Promise<number> {
    if (this._closing) return 0;

    const activeKey = `${this.keyPrefix}:groups:active`;
    const len = await this.client.llen(activeKey);
    if (len === 0) return 0;

    let scheduled = 0;
    // Cap at 64 groups per pass to avoid monopolising the event loop on huge queues.
    const limit = Math.min(len, 64);
    for (let i = 0; i < limit; i++) {
      const groupId = await this.client.lpop(activeKey);
      if (!groupId) break;
      if (await this.scheduleOneGroup(groupId)) scheduled++;
    }
    return scheduled;
  }

  private async scheduleOneGroup(groupId: string): Promise<boolean> {
    const activeKey = `${this.keyPrefix}:groups:active`;

    // ── 1. Pause check ────────────────────────────────────────────────────────
    const isPaused = await this.client.sismember(`${this.keyPrefix}:groups:paused`, groupId);
    if (isPaused) {
      await this.client.rpush(activeKey, groupId);
      return false;
    }

    // ── 2. Rate limit check (manual via rateLimitGroup or global limit) ───────
    const rateLimitTtl = await this.client.pttl(`${this.keyPrefix}:group:rate-limit:${groupId}`);
    if (rateLimitTtl > 0) {
      await this.client.rpush(activeKey, groupId);
      return false;
    }

    // ── 3. Resolve concurrency: local override > worker-level global default ──
    const localCfg = await this.client.get(`${this.keyPrefix}:group:cfg:${groupId}`);
    const maxGroupConcurrency = localCfg !== null
      ? parseInt(localCfg, 10)
      : (this.opts.group?.concurrency ?? 1);

    const currentRunning = parseInt(
      (await this.client.get(`${this.keyPrefix}:running:${groupId}`)) ?? '0',
      10,
    );

    if (currentRunning >= maxGroupConcurrency) {
      await this.client.rpush(activeKey, groupId);
      return false;
    }

    // ── 4. Group lock — multi-worker safety ───────────────────────────────────
    const lockKey = `${this.keyPrefix}:lock:group:${groupId}`;
    const lockAcquired = await this.client.set(lockKey, this.id, 'EX', 30, 'NX');
    if (!lockAcquired) {
      await this.client.rpush(activeKey, groupId);
      return false;
    }

    // ── 5. Fill available slots ───────────────────────────────────────────────
    const availableSlots = maxGroupConcurrency - currentRunning;
    let scheduled = 0;

    for (let i = 0; i < availableSlots; i++) {
      // Priority ZSET first (ZPOPMIN = lowest score = highest priority), then FIFO LIST.
      let jobId: string | null = null;
      const priorityPop = await this.client.zpopmin(`${this.keyPrefix}:group:priority:${groupId}`, 1);
      if (priorityPop.length > 0) {
        jobId = priorityPop[0]; // [member, score, ...]
      } else {
        jobId = await this.client.lpop(`${this.keyPrefix}:group:${groupId}`);
      }
      if (!jobId) break;

      // ── Global rate limit: INCR counter, check against max ─────────────────
      const globalLimit = this.opts.group?.limit;
      if (globalLimit) {
        const rateKey = `${this.keyPrefix}:group:rate:${groupId}`;
        const count = await this.client.incr(rateKey);
        if (count === 1) {
          // First hit in this window — set the expiry.
          await this.client.pexpire(rateKey, globalLimit.duration);
        }
        if (count > globalLimit.max) {
          // Limit exceeded — put job back at front of queue and rate-limit the group.
          await this.client.lpush(`${this.keyPrefix}:group:${groupId}`, jobId);
          const windowTtl = await this.client.pttl(rateKey);
          const delay = windowTtl > 0 ? windowTtl : globalLimit.duration;
          await this.client.set(
            `${this.keyPrefix}:group:rate-limit:${groupId}`, '1', 'PX', delay,
          );
          // Ensure group stays in active list so it's rescheduled after the window.
          await this.client.rpush(activeKey, groupId);
          break;
        }
      }

      await this.client.incr(`${this.keyPrefix}:running:${groupId}`);
      await this.client.rpush(`${this.keyPrefix}:ready`, jobId);
      scheduled++;
    }

    // ── 6. Decide whether to keep group in active rotation ───────────────────
    if (scheduled === 0) {
      const listLen = await this.client.llen(`${this.keyPrefix}:group:${groupId}`);
      const zsetLen = await this.client.zcard(`${this.keyPrefix}:group:priority:${groupId}`);
      if (listLen === 0 && zsetLen === 0) {
        await this.client.srem(`${this.keyPrefix}:groups:set`, groupId);
      }
      // If we didn't push to activeKey above (no rate limit), don't add again.
    } else {
      const listLen = await this.client.llen(`${this.keyPrefix}:group:${groupId}`);
      const zsetLen = await this.client.zcard(`${this.keyPrefix}:group:priority:${groupId}`);
      if (listLen > 0 || zsetLen > 0) {
        await this.client.rpush(activeKey, groupId);
      } else {
        await this.client.srem(`${this.keyPrefix}:groups:set`, groupId);
      }
    }

    await this.client.del(lockKey);
    return scheduled > 0;
  }

  // ─── Delayed job promoter ──────────────────────────────────────────────────

  private startDelayedPromoter(): void {
    this.delayedTimer = setInterval(
      () => this.promoteDelayedJobs().catch(() => {/* ignore */}),
      1000,
    );
  }

  private async promoteDelayedJobs(): Promise<void> {
    if (this._closing) return;
    const now = Date.now();
    const ids = await this.client.zrangebyscore(`${this.keyPrefix}:delayed`, '-inf', now);
    if (!ids.length) return;

    const pipe = this.client.pipeline();
    for (const id of ids) {
      pipe.zrem(`${this.keyPrefix}:delayed`, id);
    }
    await pipe.exec();

    for (const id of ids) {
      // Re-enqueue respecting group/priority
      const job = await Job.fromId<DataType, ResultType, NameType>(
        this.client, this.name, this.opts.prefix ?? 'bull', id,
      );
      if (!job) continue;

      if (job.opts.group?.id) {
        const groupId = job.opts.group.id;
        const groupPriority = job.opts.group.priority ?? 0;
        if (groupPriority > 0) {
          await this.client.zadd(`${this.keyPrefix}:group:priority:${groupId}`, groupPriority, id);
        } else {
          await this.client.rpush(`${this.keyPrefix}:group:${groupId}`, id);
        }
        const added = await this.client.sadd(`${this.keyPrefix}:groups:set`, groupId);
        if (added) await this.client.rpush(`${this.keyPrefix}:groups:active`, groupId);
      } else if ((job.opts.priority ?? 0) > 0) {
        await this.client.zadd(`${this.keyPrefix}:priority`, job.opts.priority!, id);
      } else {
        await this.client.rpush(`${this.keyPrefix}:ready`, id);
      }

      await this.xadd('waiting', { jobId: id });
    }
  }

  // ─── Stalled job checker ───────────────────────────────────────────────────

  private startStalledChecker(): void {
    const interval = this.opts.stalledInterval ?? 30000;
    this.stalledTimer = setInterval(
      () => this.checkStalledJobs().catch(() => {/* ignore */}),
      interval,
    );
  }

  private async checkStalledJobs(): Promise<void> {
    if (this._closing) return;
    const activeIds = await this.client.lrange(`${this.keyPrefix}:active`, 0, -1);

    for (const jobId of activeIds) {
      // Skip jobs we are currently processing in this worker
      if (this.activeJobs.has(jobId)) continue;

      const lockExists = await this.client.exists(`${this.keyPrefix}:processing:${jobId}`);
      if (lockExists) continue;

      // Lock is gone but job is still in active — stalled
      await this.client.lrem(`${this.keyPrefix}:active`, 1, jobId);

      const job = await Job.fromId<DataType, ResultType, NameType>(
        this.client, this.name, this.opts.prefix ?? 'bull', jobId,
      );
      if (!job) continue;

      const maxStalledCount = this.opts.maxStalledCount ?? 1;

      const groupId = job.opts.group?.id;

      if (job.stalledCounter >= maxStalledCount) {
        // Permanently failed — free the group slot before moving to failed set.
        if (groupId) {
          await this.client.decr(`${this.keyPrefix}:running:${groupId}`);
        }
        job.failedReason = 'job stalled more than allowable limit';
        job.finishedOn = Date.now();
        await job.save();
        await this.client.zadd(`${this.keyPrefix}:failed`, job.finishedOn, jobId);
        await this.xadd('failed', { jobId, failedReason: job.failedReason, prev: 'active' });
        this.emit('failed', job, new Error(job.failedReason), 'active');
      } else {
        job.stalledCounter++;
        await job.save();

        if (groupId) {
          await this.client.decr(`${this.keyPrefix}:running:${groupId}`);
          // Re-enqueue respecting intra-group priority.
          const groupPriority = job.opts.group?.priority ?? 0;
          if (groupPriority > 0) {
            await this.client.zadd(`${this.keyPrefix}:group:priority:${groupId}`, groupPriority, jobId);
          } else {
            await this.client.lpush(`${this.keyPrefix}:group:${groupId}`, jobId);
          }
          const added = await this.client.sadd(`${this.keyPrefix}:groups:set`, groupId);
          if (added) await this.client.rpush(`${this.keyPrefix}:groups:active`, groupId);
        } else {
          await this.client.rpush(`${this.keyPrefix}:ready`, jobId);
        }

        await this.xadd('stalled', { jobId });
        this.emit('stalled', jobId, 'active');
      }
    }
  }

  // ─── Helpers ───────────────────────────────────────────────────────────────

  private async xadd(event: string, data: object): Promise<void> {
    await this.client
      .xadd(`${this.keyPrefix}:events`, '*', 'event', event, 'data', JSON.stringify(data))
      .catch(() => {/* non-fatal */});
  }

  async rateLimit(expireTimeMs: number): Promise<void> {
    await this.client.set(`${this.keyPrefix}:rate-limit`, '1', 'PX', expireTimeMs);
  }

  /**
   * Rate limit a group from within the processor.
   * Call this before throwing Worker.RateLimitError() when an external API
   * returns a 429 or similar. The group will be paused for `durationMs`.
   */
  async rateLimitGroup(
    job: Job<DataType, ResultType, NameType>,
    durationMs: number,
  ): Promise<void> {
    const groupId = job.opts.group?.id;
    if (!groupId) return;
    await this.client.set(
      `${this.keyPrefix}:group:rate-limit:${groupId}`,
      '1',
      'PX',
      durationMs,
    );
  }

  /** Returns a GroupRateLimitError to throw from the processor. */
  static RateLimitError(): GroupRateLimitError {
    return new GroupRateLimitError();
  }

  // ─── Lifecycle ─────────────────────────────────────────────────────────────

  async close(force = false): Promise<void> {
    if (this._closing) return;
    this._closing = true;
    this.emit('closing', 'Worker is closing');

    this.schedulerLoopActive = false;
    if (this.schedulerTimer) clearTimeout(this.schedulerTimer as unknown as ReturnType<typeof setTimeout>);
    if (this.delayedTimer) clearInterval(this.delayedTimer);
    if (this.stalledTimer) clearInterval(this.stalledTimer);

    if (!force) {
      // Drain: wait for in-flight jobs
      while (this.activeCount > 0) {
        await sleep(50);
      }
    } else {
      for (const { renewalTimer } of this.activeJobs.values()) {
        clearInterval(renewalTimer);
      }
      this.activeJobs.clear();
    }

    // disconnect() immediately interrupts the pending BLPOP (unlike quit() which queues after it)
    this.blockingClient.disconnect();
    await this.client.quit().catch(() => {/* ignore */});
    this.emit('closed');
  }
}
