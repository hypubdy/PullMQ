import { EventEmitter } from 'events';
import type Redis from 'ioredis';
import type { WorkerOptions, Processor, JobsOptions } from './types';
import { Job } from './job';
import { createClient } from './connection';
import { UnrecoverableError, GroupRateLimitError } from './errors';
import { scripted } from './scripts';
import { promoteDueDelayedJobs } from './promotion';

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
  // Guards against promoteDelayedJobs() overlapping itself: setInterval fires
  // on a fixed schedule regardless of whether the previous async call finished.
  private promotingDelayed = false;
  // Semaphore: mainLoop waiters blocked on concurrency limit
  private slotWaiters: Array<() => void> = [];
  // Jobs seen in :active without a processing lock on the last stalled scan —
  // reclaimed only if still lockless on the next scan (see checkStalledJobs).
  private stalledSuspects = new Set<string>();

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

      // ── Job pickup (non-blocking first, then BLMOVE) ───────────────────────
      // Pickup moves :ready → :active atomically (LMOVE / Lua zpopmin+rpush),
      // so a crash right after pickup leaves the job visible in :active where
      // the stalled checker recovers it — it can never vanish from every
      // structure at once the way the old LPOP-then-RPUSH two-step allowed.
      let jobId = await this.nextFromPriority();
      if (!jobId) {
        jobId = await this.client.lmove(
          `${this.keyPrefix}:ready`, `${this.keyPrefix}:active`, 'LEFT', 'RIGHT',
        );
      }

      if (jobId) {
        // Claim the slot before firing so activeCount is accurate during the call.
        this.activeCount++;
        this.processJob(jobId).catch((err) => this.emit('error', err));
        continue;
      }

      // Queue is empty — block until a job arrives (or timeout).
      const timeout = this.opts.drainDelay ?? 5;
      const moved = await this.blockingClient
        .blmove(`${this.keyPrefix}:ready`, `${this.keyPrefix}:active`, 'LEFT', 'RIGHT', timeout)
        .catch(() => null);

      if (!moved) {
        this.emit('drained');
      } else {
        this.activeCount++;
        this.processJob(moved).catch((err) => this.emit('error', err));
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
    // Atomic zpopmin + rpush :active — see the pickup comment in mainLoop.
    return scripted(this.client).pmqPickupPriority(
      `${this.keyPrefix}:priority`, `${this.keyPrefix}:active`,
    );
  }

  // ─── Single job lifecycle ──────────────────────────────────────────────────

  private async processJob(jobId: string): Promise<void> {
    // activeCount was already incremented by mainLoop before this call.
    const token = `${this.id}:${jobId}`;

    try {
      // The job is already in :active (atomic pickup). Set the processing lock
      // FIRST — before even loading the hash — so another worker's stalled
      // checker sees a locked job, not a stall, in the pickup-to-activation gap.
      const lockDuration = this.opts.lockDuration ?? 30000;
      await this.client.set(`${this.keyPrefix}:processing:${jobId}`, token, 'PX', lockDuration);

      const job = await Job.fromId<DataType, ResultType, NameType>(
        this.client,
        this.name,
        this.opts.prefix ?? 'bull',
        jobId,
      );

      if (!job) {
        await this.abandonPickedUpJob(jobId, token);
        return;
      }

      const activated = await this.lockAndActivate(job, token);
      if (!activated) {
        // Queue.remove() won the race between pickup and activation;
        // lockAndActivate already undid the pickup bookkeeping.
        return;
      }

      try {
        if (!this.processor) throw new Error('No processor defined');

        const abortController = new AbortController();
        const result = await this.processor(job, token, abortController.signal);

        job.returnvalue = result;
        job.finishedOn = Date.now();

        if (await job.saveIfExists()) {
          await this.onCompleted(job, token);
        } else {
          // Job was removed mid-processing — drop the result instead of
          // resurrecting the hash and recording a completion for a removed job.
          this.inMemoryCleanup(job.id);
          await this.finishJob({ jobId: job.id, token, groupId: job.opts.group?.id, mode: 'none' });
        }

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
      // Single release point — the try block must never call releaseSlot()
      // itself, or the slot is double-released and activeCount goes negative,
      // silently raising the effective concurrency.
      this.releaseSlot();
    }
  }

  // Atomically detach a job from :active + its lock and attach it to its
  // destination in ONE Lua call (pmqFinishJob). Every transition out of
  // :active must go through here: doing detach and attach as separate
  // round-trips loses the job if the process dies in between.
  private async finishJob(opts: {
    jobId: string;
    token: string;
    groupId?: string;
    mode: 'zadd' | 'list' | 'none';
    destKey?: string;
    score?: number;
    pushCmd?: 'lpush' | 'rpush';
    register?: boolean;
    guard?: boolean;
  }): Promise<number> {
    const groupId = opts.groupId ?? '';
    return scripted(this.client).pmqFinishJob(
      `${this.keyPrefix}:active`,
      `${this.keyPrefix}:processing:${opts.jobId}`,
      `${this.keyPrefix}:group:job-map`,
      // Dummy key when there is no group — the script never touches it then.
      `${this.keyPrefix}:running:${groupId || '-'}`,
      opts.destKey ?? `${this.keyPrefix}:ready`,
      `${this.keyPrefix}:groups:set`,
      `${this.keyPrefix}:groups:active`,
      opts.jobId, opts.token, groupId, opts.mode,
      String(opts.score ?? 0), opts.pushCmd ?? 'rpush',
      opts.register ? '1' : '0', opts.guard ? '1' : '0',
    );
  }

  private inMemoryCleanup(jobId: string): void {
    const entry = this.activeJobs.get(jobId);
    if (entry) {
      clearInterval(entry.renewalTimer);
      this.activeJobs.delete(jobId);
    }
  }

  // A picked-up job turned out to have no hash (removed or evicted after
  // dispatch). Undo the pickup bookkeeping and release its group slot.
  private async abandonPickedUpJob(jobId: string, token: string): Promise<void> {
    // The hash is gone, so the groupId must be recovered from the dispatch map.
    const groupId = await this.client.hget(`${this.keyPrefix}:group:job-map`, jobId);
    await this.finishJob({ jobId, token, groupId: groupId ?? '', mode: 'none' });
  }

  private async onRateLimited(
    job: Job<DataType, ResultType, NameType>,
    token: string,
  ): Promise<void> {
    this.inMemoryCleanup(job.id);
    const groupId = job.opts.group?.id;

    // A job removed while processing must not have its id resurrected into a
    // queue structure — the id would dangle with no hash behind it.
    const stillExists = await this.client.exists(`${this.keyPrefix}:job:${job.id}`);
    if (!stillExists) {
      await this.finishJob({ jobId: job.id, token, groupId, mode: 'none' });
      return;
    }

    if (groupId) {
      // Re-enqueue at the FRONT (lpush) so the job is next when the limit
      // lifts; re-admission never re-checks maxSize (the job was already in).
      const groupPriority = job.opts.group?.priority ?? 0;
      await this.finishJob(groupPriority > 0
        ? {
            jobId: job.id, token, groupId, mode: 'zadd',
            destKey: `${this.keyPrefix}:group:priority:${groupId}`,
            score: groupPriority, register: true,
          }
        : {
            jobId: job.id, token, groupId, mode: 'list',
            destKey: `${this.keyPrefix}:group:${groupId}`,
            pushCmd: 'lpush', register: true,
          });
    } else {
      await this.finishJob({ jobId: job.id, token, mode: 'list' });
    }
    await this.xadd('rate-limited', { jobId: job.id });
  }

  // The processing lock and the :active entry already exist by the time this
  // runs (set during atomic pickup / processJob). Returns false when the job
  // hash disappeared in the meantime — i.e. the job was removed — in which
  // case the pickup bookkeeping is undone and processing must not start.
  private async lockAndActivate(
    job: Job<DataType, ResultType, NameType>,
    token: string,
  ): Promise<boolean> {
    const lockDuration = this.opts.lockDuration ?? 30000;

    job.processedOn = Date.now();
    job.attemptsStarted++;
    job.attemptsMade++;
    if (!(await job.saveIfExists())) {
      await this.finishJob({ jobId: job.id, token, groupId: job.opts.group?.id, mode: 'none' });
      return false;
    }

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
    return true;
  }

  // A job was dispatched from scheduleOneGroup() (which already INCR'd
  // running:{groupId}) but its hash is gone by the time we tried to load it,
  // so cleanup() never runs and the group slot would otherwise leak forever.
  // Recover the groupId from the dispatch-time map and release the slot here.
  private async releaseGroupSlotForMissingJob(jobId: string): Promise<void> {
    const groupId = await this.client.hget(`${this.keyPrefix}:group:job-map`, jobId);
    if (!groupId) return;
    // Gate the decrement on HDEL's return value — same ownership pattern as
    // cleanup() and checkStalledJobs() — so two concurrent callers racing on
    // the same jobId (e.g. this method invoked from both processJob's and
    // checkStalledJobs' missing-job branches) can't both decrement.
    const owned = await this.client.hdel(`${this.keyPrefix}:group:job-map`, jobId);
    if (owned > 0) {
      const remaining = await this.client.decr(`${this.keyPrefix}:running:${groupId}`);
      if (remaining < 0) {
        await this.client.set(`${this.keyPrefix}:running:${groupId}`, '0');
      }
    }
  }

  private async onCompleted(
    job: Job<DataType, ResultType, NameType>,
    token: string,
  ): Promise<void> {
    this.inMemoryCleanup(job.id);
    const groupId = job.opts.group?.id;
    const policy = job.opts.removeOnComplete ?? this.opts.removeOnComplete;

    if (policy === true) {
      await this.finishJob({ jobId: job.id, token, groupId, mode: 'none' });
      await job.remove();
    } else {
      await this.finishJob({
        jobId: job.id, token, groupId, mode: 'zadd',
        destKey: `${this.keyPrefix}:completed`,
        score: job.finishedOn ?? Date.now(),
      });
      await this.applyKeepTrims(`${this.keyPrefix}:completed`, policy);
    }

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
    const groupId = job.opts.group?.id;

    this.inMemoryCleanup(job.id);

    job.failedReason = err.message;
    if (err.stack) job.stacktrace.push(err.stack);
    job.finishedOn = Date.now();
    if (!(await job.saveIfExists())) {
      // Job was removed mid-processing — nothing to record or retry.
      await this.finishJob({ jobId: job.id, token, groupId, mode: 'none' });
      return;
    }

    const canRetry = !isUnrecoverable && job.attemptsMade < maxAttempts;

    if (canRetry) {
      const delay = this.calcBackoff(job);
      if (delay > 0) {
        await this.finishJob({
          jobId: job.id, token, groupId, mode: 'zadd',
          destKey: `${this.keyPrefix}:delayed`,
          score: Date.now() + delay,
        });
        await this.xadd('delayed', { jobId: job.id, delay });
      } else if (groupId) {
        // Re-enqueue with group and priority awareness.
        const groupPriority = job.opts.group?.priority ?? 0;
        await this.finishJob(groupPriority > 0
          ? {
              jobId: job.id, token, groupId, mode: 'zadd',
              destKey: `${this.keyPrefix}:group:priority:${groupId}`,
              score: groupPriority, register: true,
            }
          : {
              jobId: job.id, token, groupId, mode: 'list',
              destKey: `${this.keyPrefix}:group:${groupId}`,
              pushCmd: 'rpush', register: true,
            });
      } else {
        await this.finishJob({ jobId: job.id, token, mode: 'list' });
      }
    } else {
      const policy = job.opts.removeOnFail ?? this.opts.removeOnFail;
      if (policy === true) {
        await this.finishJob({ jobId: job.id, token, groupId, mode: 'none' });
        await job.remove();
      } else {
        await this.finishJob({
          jobId: job.id, token, groupId, mode: 'zadd',
          destKey: `${this.keyPrefix}:failed`,
          score: job.finishedOn ?? Date.now(),
        });
        await this.applyKeepTrims(`${this.keyPrefix}:failed`, policy);
      }
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

  // Post-finish trimming for keep-count / keep-age policies. The ZADD into the
  // finished set itself happens atomically inside finishJob().
  private async applyKeepTrims(
    setKey: string,
    policy: boolean | number | { count?: number; age?: number } | undefined,
  ): Promise<void> {
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

  // Schedule one batch across the groups in rotation, in a single pass.
  // Returns the number of groups for which at least one job was dispatched.
  private async scheduleGroupBatch(): Promise<number> {
    if (this._closing) return 0;

    const activeKey = `${this.keyPrefix}:groups:active`;
    const len = await this.client.llen(activeKey);
    if (len === 0) return 0;

    let scheduled = 0;
    // Cap at 64 groups per pass to avoid monopolising the event loop on huge queues.
    const limit = Math.min(len, 64);
    const seen = new Set<string>();
    for (let i = 0; i < limit; i++) {
      // Non-destructive rotation (head → tail in one atomic LMOVE): the group
      // never leaves the list while it is being examined, so an error or crash
      // mid-pass cannot drop it out of rotation the way the old LPOP-then-
      // maybe-RPUSH pattern could.
      const groupId = await this.client.lmove(activeKey, activeKey, 'LEFT', 'RIGHT');
      if (!groupId) break;
      if (seen.has(groupId)) continue; // duplicate rotation entry — purged on retire
      seen.add(groupId);
      if (await this.scheduleOneGroup(groupId)) scheduled++;
    }
    return scheduled;
  }

  // Rotation model: the group stays in groups:active the whole time (the batch
  // loop rotates it head → tail); this method never pushes it back. The only
  // structural change is atomic retirement (empty-check + LREM + SREM in one
  // Lua call) once the group has no waiting jobs left — a concurrent enqueue
  // either lands before the check (group kept) or re-registers the group
  // itself afterwards via the enqueue script's SADD.
  private async scheduleOneGroup(groupId: string): Promise<boolean> {
    const listKey = `${this.keyPrefix}:group:${groupId}`;
    const zsetKey = `${this.keyPrefix}:group:priority:${groupId}`;

    // ── 1. Pause check ────────────────────────────────────────────────────────
    const isPaused = await this.client.sismember(`${this.keyPrefix}:groups:paused`, groupId);
    if (isPaused) return false;

    // ── 2. Rate limit check (manual via rateLimitGroup or global limit) ───────
    const rateLimitTtl = await this.client.pttl(`${this.keyPrefix}:group:rate-limit:${groupId}`);
    if (rateLimitTtl > 0) return false;

    // ── 3. Resolve concurrency: local override > worker-level global default ──
    const localCfg = await this.client.get(`${this.keyPrefix}:group:cfg:${groupId}`);
    const maxGroupConcurrency = localCfg !== null
      ? parseInt(localCfg, 10)
      : (this.opts.group?.concurrency ?? 1);

    const currentRunning = parseInt(
      (await this.client.get(`${this.keyPrefix}:running:${groupId}`)) ?? '0',
      10,
    );
    if (currentRunning >= maxGroupConcurrency) return false;

    // ── 4. Group lock — reduces cross-worker contention. Correctness does NOT
    // depend on it: the dispatch script enforces the concurrency ceiling
    // atomically, so even an expired lock cannot over-provision the group.
    const lockKey = `${this.keyPrefix}:lock:group:${groupId}`;
    const lockAcquired = await this.client.set(lockKey, this.id, 'EX', 30, 'NX');
    if (!lockAcquired) return false;

    let scheduled = 0;
    try {
      // ── 5. Fill available slots ─────────────────────────────────────────────
      // Re-read currentRunning inside the lock: another worker may have
      // incremented it between the pre-lock GET (step 3) and now.
      const freshRunning = parseInt(
        (await this.client.get(`${this.keyPrefix}:running:${groupId}`)) ?? '0',
        10,
      );
      if (freshRunning >= maxGroupConcurrency) return false;
      const availableSlots = maxGroupConcurrency - freshRunning;

      for (let i = 0; i < availableSlots; i++) {
        // ── Global rate limit: consume a window slot BEFORE popping, so there
        // is never a popped-but-not-dispatched job to lose in a crash. ───────
        const globalLimit = this.opts.group?.limit;
        let rateSlotTaken = false;
        if (globalLimit) {
          const rateKey = `${this.keyPrefix}:group:rate:${groupId}`;
          const count = await this.client.incr(rateKey);
          if (count === 1) {
            // First hit in this window — set the expiry.
            await this.client.pexpire(rateKey, globalLimit.duration);
          }
          if (count > globalLimit.max) {
            // Limit exceeded — rate-limit the group until the window expires.
            const windowTtl = await this.client.pttl(rateKey);
            const delay = windowTtl > 0 ? windowTtl : globalLimit.duration;
            await this.client.set(
              `${this.keyPrefix}:group:rate-limit:${groupId}`, '1', 'PX', delay,
            );
            break;
          }
          rateSlotTaken = true;
        }

        // Atomic pop + dispatch: take the next job (priority zset first, then
        // FIFO list), enforce the ceiling, INCR running:{groupId}, record
        // group:job-map ownership (kept independent of the job hash so the
        // slot can still be released if the hash disappears — see
        // releaseGroupSlotForMissingJob()) and RPUSH to :ready in ONE Lua
        // call. There is no popped-but-undispatched state for a crash to hit.
        const jobId = await scripted(this.client).pmqPopDispatch(
          zsetKey, listKey,
          `${this.keyPrefix}:running:${groupId}`,
          `${this.keyPrefix}:group:job-map`,
          `${this.keyPrefix}:ready`,
          groupId,
          String(maxGroupConcurrency),
        );
        if (!jobId) {
          // Group empty (or ceiling hit despite our lock) — hand back the
          // rate-window slot we reserved for a job that never materialised.
          if (rateSlotTaken) {
            await this.client.decr(`${this.keyPrefix}:group:rate:${groupId}`).catch(() => {});
          }
          break;
        }
        scheduled++;
      }

      // ── 6. Retire the group if it has nothing left to schedule ─────────────
      await scripted(this.client).pmqRetireGroup(
        listKey, zsetKey,
        `${this.keyPrefix}:groups:set`, `${this.keyPrefix}:groups:active`,
        groupId,
      );
      return scheduled > 0;
    } finally {
      await this.client.del(lockKey).catch(() => {/* expires on its own */});
    }
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
    // Prevent this call from overlapping a still-running previous call (this
    // worker's own setInterval tick) in addition to the cross-worker guard
    // below — otherwise the same id could be re-enqueued by both overlapping
    // calls before either sees the other's ZREM.
    if (this.promotingDelayed) return;
    this.promotingDelayed = true;
    try {
      await this.promoteDelayedJobsOnce();
    } finally {
      this.promotingDelayed = false;
    }
  }

  private async promoteDelayedJobsOnce(): Promise<void> {
    // Shared with Queue.promoteJobs() so claim semantics, routing and maxSize
    // enforcement can never drift apart between the two paths.
    const promoted = await promoteDueDelayedJobs(
      this.client, this.name, this.opts.prefix ?? 'bull',
    );
    for (const id of promoted) {
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

    // Suspects seen lockless on the PREVIOUS scan; rebuilt every scan.
    const priorSuspects = this.stalledSuspects;
    const nextSuspects = new Set<string>();

    for (const jobId of activeIds) {
      // Skip jobs we are currently processing in this worker
      if (this.activeJobs.has(jobId)) continue;

      const lockExists = await this.client.exists(`${this.keyPrefix}:processing:${jobId}`);
      if (lockExists) continue;

      // Grace pass: a job freshly moved into :active sits lockless for one
      // round-trip before its worker sets the processing lock. Reclaiming on
      // the first lockless sighting turns that sliver into a false stall — a
      // second copy of a RUNNING job — which cascades (the copies share one
      // lock key). Only a job seen lockless on two consecutive scans, a full
      // stalledInterval apart, is genuinely dead.
      if (!priorSuspects.has(jobId)) {
        nextSuspects.add(jobId);
        continue;
      }

      // Lock gone across two scans — stalled for real. Reclaim ATOMICALLY via
      // the guarded finish script: reading the job first (while it is still in
      // :active) then moving it in one Lua call means a checker killed
      // mid-reclaim cannot strand the job in no structure at all. The guard
      // also rejects the reclaim if the lock reappeared or another checker won.
      const job = await Job.fromId<DataType, ResultType, NameType>(
        this.client, this.name, this.opts.prefix ?? 'bull', jobId,
      );
      if (!job) {
        const groupId = await this.client.hget(`${this.keyPrefix}:group:job-map`, jobId);
        await this.finishJob({ jobId, token: '', groupId: groupId ?? '', mode: 'none', guard: true });
        continue;
      }

      const maxStalledCount = this.opts.maxStalledCount ?? 1;
      const groupId = job.opts.group?.id;

      if (job.stalledCounter >= maxStalledCount) {
        job.failedReason = 'job stalled more than allowable limit';
        job.finishedOn = Date.now();
        // Saved before the reclaim; if the guard then rejects (job came back to
        // life), the live worker's own save overwrites this on real completion.
        if (!(await job.saveIfExists())) {
          await this.finishJob({ jobId, token: '', groupId, mode: 'none', guard: true });
          continue;
        }
        const reclaimed = await this.finishJob({
          jobId, token: '', groupId, mode: 'zadd', guard: true,
          destKey: `${this.keyPrefix}:failed`, score: job.finishedOn,
        });
        if (reclaimed === 0) continue;
        await this.xadd('failed', { jobId, failedReason: job.failedReason, prev: 'active' });
        this.emit('failed', job, new Error(job.failedReason), 'active');
      } else {
        job.stalledCounter++;
        if (!(await job.saveIfExists())) {
          // Removed while stalled — release the slot and don't resurrect the id.
          await this.finishJob({ jobId, token: '', groupId, mode: 'none', guard: true });
          continue;
        }

        // Re-enqueue at the FRONT, respecting intra-group priority.
        const groupPriority = job.opts.group?.priority ?? 0;
        const reclaimed = groupId
          ? await this.finishJob(groupPriority > 0
              ? {
                  jobId, token: '', groupId, mode: 'zadd', guard: true,
                  destKey: `${this.keyPrefix}:group:priority:${groupId}`,
                  score: groupPriority, register: true,
                }
              : {
                  jobId, token: '', groupId, mode: 'list', guard: true,
                  destKey: `${this.keyPrefix}:group:${groupId}`,
                  pushCmd: 'lpush', register: true,
                })
          : await this.finishJob({ jobId, token: '', mode: 'list', guard: true });
        if (reclaimed === 0) continue;

        await this.xadd('stalled', { jobId });
        this.emit('stalled', jobId, 'active');
      }
    }

    this.stalledSuspects = nextSuspects;
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
