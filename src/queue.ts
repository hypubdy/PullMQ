import { EventEmitter } from 'events';
import type Redis from 'ioredis';
import type {
  QueueOptions,
  JobsOptions,
  JobCounts,
  JobState,
  DefaultJobOptions,
} from './types';
import { Job } from './job';
import { createClient } from './connection';
import { GroupMaxSizeExceededError } from './errors';
import { LUA_GROUP_ENQUEUE } from './scripts';

export class Queue<
  DataType = unknown,
  ResultType = unknown,
  NameType extends string = string,
> extends EventEmitter {
  readonly name: string;
  readonly opts: Required<Pick<QueueOptions, 'prefix'>> & QueueOptions;
  readonly defaultJobOptions: DefaultJobOptions;

  private client: Redis;

  constructor(name: string, opts?: QueueOptions) {
    super();
    this.name = name;
    this.opts = {
      prefix: 'bull',
      defaultJobOptions: {},
      ...opts,
      connection: opts?.connection ?? { host: '127.0.0.1', port: 6379 },
    };
    this.defaultJobOptions = this.opts.defaultJobOptions ?? {};
    this.client = createClient(this.opts.connection);
    this.client.on('error', (err) => this.emit('error', err));
  }

  get keyPrefix(): string {
    return `${this.opts.prefix}:${this.name}`;
  }

  async waitUntilReady(): Promise<Redis> {
    await this.client.ping();
    return this.client;
  }

  // ─── Job addition ────────────────────────────────────────────────────────────

  async add(name: NameType, data: DataType, opts?: JobsOptions): Promise<Job<DataType, ResultType, NameType>> {
    const jobOpts: JobsOptions = { ...this.defaultJobOptions, ...opts };
    const job = new Job<DataType, ResultType, NameType>(
      this.client,
      this.name,
      this.opts.prefix,
      name,
      data,
      jobOpts,
    );
    await job.save();
    await this.enqueue(job);
    await this.xadd('added', { jobId: job.id, name: job.name });
    this.emit('waiting', job);
    return job;
  }

  async addBulk(
    jobs: Array<{ name: NameType; data: DataType; opts?: JobsOptions }>,
  ): Promise<Job<DataType, ResultType, NameType>[]> {
    return Promise.all(jobs.map((j) => this.add(j.name, j.data, j.opts)));
  }

  private async enqueue(job: Job<DataType, ResultType, NameType>): Promise<void> {
    const delay = job.opts.delay ?? 0;
    const groupId = job.opts.group?.id;
    const priority = job.opts.priority ?? 0;

    if (delay > 0) {
      await this.client.zadd(`${this.keyPrefix}:delayed`, Date.now() + delay, job.id);
      return;
    }

    if (groupId) {
      const maxSize = job.opts.group?.maxSize;
      const groupPriority = job.opts.group?.priority ?? 0;

      // Atomic size-check + enqueue via Lua: eliminates the TOCTOU race where
      // two concurrent add() calls both pass a non-atomic llen+zcard check.
      // maxSize=undefined is passed as -1 ("unlimited") so it stays distinct
      // from an explicit maxSize:0 ("reject every add").
      const ok = await this.client.eval(
        LUA_GROUP_ENQUEUE,
        2,
        `${this.keyPrefix}:group:${groupId}`,
        `${this.keyPrefix}:group:priority:${groupId}`,
        String(maxSize ?? -1),
        job.id,
        String(groupPriority),
      );
      if (ok === 0) throw new GroupMaxSizeExceededError(groupId, maxSize ?? 0);

      // ── Register group in round-robin tracking ────────────────────────────
      const added = await this.client.sadd(`${this.keyPrefix}:groups:set`, groupId);
      if (added === 1) {
        await this.client.rpush(`${this.keyPrefix}:groups:active`, groupId);
      }
      return;
    }

    if (priority > 0) {
      await this.client.zadd(`${this.keyPrefix}:priority`, priority, job.id);
    } else {
      await this.client.rpush(`${this.keyPrefix}:ready`, job.id);
    }
  }

  // ─── Job retrieval ───────────────────────────────────────────────────────────

  async getJob(jobId: string): Promise<Job<DataType, ResultType, NameType> | null> {
    return Job.fromId<DataType, ResultType, NameType>(
      this.client,
      this.name,
      this.opts.prefix,
      jobId,
    );
  }

  async getJobs(
    types: JobState | JobState[],
    start = 0,
    end = -1,
    asc = false,
  ): Promise<Job<DataType, ResultType, NameType>[]> {
    const typeArr = Array.isArray(types) ? types : [types];
    const ids: string[] = [];

    for (const type of typeArr) {
      const typeIds = await this.getJobIdsByType(type, start, end, asc);
      ids.push(...typeIds);
    }

    const jobs = await Promise.all(ids.map((id) => this.getJob(id)));
    return jobs.filter((j): j is Job<DataType, ResultType, NameType> => j !== null);
  }

  private async getJobIdsByType(type: JobState, start: number, end: number, asc: boolean): Promise<string[]> {
    switch (type) {
      case 'waiting':
        return this.client.lrange(`${this.keyPrefix}:ready`, start, end);
      case 'active':
        return this.client.lrange(`${this.keyPrefix}:active`, start, end);
      case 'completed':
        return asc
          ? this.client.zrange(`${this.keyPrefix}:completed`, start, end)
          : this.client.zrevrange(`${this.keyPrefix}:completed`, start, end);
      case 'failed':
        return asc
          ? this.client.zrange(`${this.keyPrefix}:failed`, start, end)
          : this.client.zrevrange(`${this.keyPrefix}:failed`, start, end);
      case 'delayed':
        return asc
          ? this.client.zrange(`${this.keyPrefix}:delayed`, start, end)
          : this.client.zrevrange(`${this.keyPrefix}:delayed`, start, end);
      case 'prioritized':
        return this.client.zrange(`${this.keyPrefix}:priority`, start, end);
      default:
        return [];
    }
  }

  async getJobCounts(...types: JobState[]): Promise<JobCounts> {
    const all = types.length === 0
      ? (['active', 'completed', 'failed', 'delayed', 'waiting', 'paused'] as JobState[])
      : types;

    const counts: JobCounts = { active: 0, completed: 0, failed: 0, delayed: 0, waiting: 0, paused: 0 };

    await Promise.all(all.map(async (type) => {
      switch (type) {
        case 'waiting':
          counts.waiting = await this.client.llen(`${this.keyPrefix}:ready`);
          break;
        case 'active':
          counts.active = await this.client.llen(`${this.keyPrefix}:active`);
          break;
        case 'completed':
          counts.completed = await this.client.zcard(`${this.keyPrefix}:completed`);
          break;
        case 'failed':
          counts.failed = await this.client.zcard(`${this.keyPrefix}:failed`);
          break;
        case 'delayed':
          counts.delayed = await this.client.zcard(`${this.keyPrefix}:delayed`);
          break;
        case 'paused':
          counts.paused = await this.client.llen(`${this.keyPrefix}:paused-ready`);
          break;
        case 'prioritized':
          counts.prioritized = await this.client.zcard(`${this.keyPrefix}:priority`);
          break;
      }
    }));

    return counts;
  }

  async getActiveCount(): Promise<number> {
    return this.client.llen(`${this.keyPrefix}:active`);
  }

  async getWaitingCount(): Promise<number> {
    return this.client.llen(`${this.keyPrefix}:ready`);
  }

  async getDelayedCount(): Promise<number> {
    return this.client.zcard(`${this.keyPrefix}:delayed`);
  }

  async getCompletedCount(): Promise<number> {
    return this.client.zcard(`${this.keyPrefix}:completed`);
  }

  async getFailedCount(): Promise<number> {
    return this.client.zcard(`${this.keyPrefix}:failed`);
  }

  // ─── Queue-level controls ────────────────────────────────────────────────────

  async pause(): Promise<void> {
    await this.client.set(`${this.keyPrefix}:paused`, '1');
    await this.xadd('paused', {});
    this.emit('paused');
  }

  async resume(): Promise<void> {
    await this.client.del(`${this.keyPrefix}:paused`);
    await this.xadd('resumed', {});
    this.emit('resumed');
  }

  async isPaused(): Promise<boolean> {
    return (await this.client.exists(`${this.keyPrefix}:paused`)) === 1;
  }

  async drain(delayed = false): Promise<void> {
    const pipe = this.client.pipeline();
    pipe.del(`${this.keyPrefix}:ready`);
    if (delayed) pipe.del(`${this.keyPrefix}:delayed`);
    await pipe.exec();
  }

  async obliterate(opts: { force?: boolean; count?: number } = {}): Promise<void> {
    let cursor = '0';
    do {
      const [next, keys] = await this.client.scan(cursor, 'MATCH', `${this.keyPrefix}:*`, 'COUNT', 100);
      cursor = next;
      if (keys.length > 0) await this.client.del(...keys);
    } while (cursor !== '0');
  }

  async clean(
    grace: number,
    limit: number,
    type: 'completed' | 'failed' | 'active' | 'wait' | 'delayed' | 'paused' = 'completed',
  ): Promise<string[]> {
    const redisType = type === 'wait' ? 'completed' : type;
    const cutoff = Date.now() - grace;
    const key = `${this.keyPrefix}:${redisType}`;

    const ids = await this.client.zrangebyscore(key, '-inf', cutoff, 'LIMIT', 0, limit);
    if (ids.length === 0) return ids;

    const pipe = this.client.pipeline();
    for (const id of ids) {
      pipe.zrem(key, id);
      pipe.del(`${this.keyPrefix}:job:${id}`);
    }
    await pipe.exec();

    this.emit('cleaned', ids, type);
    return ids;
  }

  async remove(jobId: string, opts: { removeChildren?: boolean } = {}): Promise<number> {
    const job = await this.getJob(jobId);
    if (!job) return 0;
    await job.remove();

    const groupId = job.opts.group?.id;

    const pipe = this.client.pipeline();
    pipe.zrem(`${this.keyPrefix}:completed`, jobId);
    pipe.zrem(`${this.keyPrefix}:failed`, jobId);
    pipe.zrem(`${this.keyPrefix}:delayed`, jobId);
    pipe.zrem(`${this.keyPrefix}:priority`, jobId);
    pipe.lrem(`${this.keyPrefix}:ready`, 0, jobId);
    pipe.lrem(`${this.keyPrefix}:active`, 0, jobId);
    if (groupId) {
      pipe.lrem(`${this.keyPrefix}:group:${groupId}`, 0, jobId);
      pipe.zrem(`${this.keyPrefix}:group:priority:${groupId}`, jobId);
    }
    await pipe.exec();

    // The job may have already been dispatched (running:{groupId} incremented,
    // group:job-map[jobId] set) before being removed. Since it's now gone from
    // :ready/:active, no worker will ever discover it's missing and release the
    // slot itself — release it here so the group's concurrency count doesn't leak.
    if (groupId) {
      const owned = await this.client.hdel(`${this.keyPrefix}:group:job-map`, jobId);
      if (owned > 0) {
        const remaining = await this.client.decr(`${this.keyPrefix}:running:${groupId}`);
        if (remaining < 0) {
          await this.client.set(`${this.keyPrefix}:running:${groupId}`, '0');
        }
      }
    }

    await this.xadd('removed', { jobId, prev: 'unknown' });
    return 1;
  }

  async retryJobs(opts: { count?: number; state?: 'completed' | 'failed'; timestamp?: number } = {}): Promise<void> {
    const state = opts.state ?? 'failed';
    const count = opts.count ?? 1000;
    const key = `${this.keyPrefix}:${state}`;
    const ids = await this.client.zrange(key, 0, count - 1);
    await Promise.all(ids.map((id) => this.getJob(id).then((job) => job?.retry(state))));
  }

  async promoteJobs(opts: { count?: number } = {}): Promise<void> {
    const count = opts.count ?? 1000;
    const now = Date.now();
    const ids = await this.client.zrangebyscore(
      `${this.keyPrefix}:delayed`, '-inf', now, 'LIMIT', 0, count,
    );
    const pipe = this.client.pipeline();
    for (const id of ids) {
      pipe.zrem(`${this.keyPrefix}:delayed`, id);
      pipe.rpush(`${this.keyPrefix}:ready`, id);
    }
    await pipe.exec();
  }

  // ─── Group controls ──────────────────────────────────────────────────────────

  /**
   * Pause a group. Workers finish any in-progress job from this group, then
   * go idle for that group until resumeGroup() is called.
   * Returns false if the group is already paused.
   */
  async pauseGroup(groupId: string): Promise<boolean> {
    const added = await this.client.sadd(`${this.keyPrefix}:groups:paused`, groupId);
    if (added === 0) return false; // already paused
    await this.xadd('group-paused', { groupId });
    return true;
  }

  /**
   * Resume a paused group.
   * Returns false if the group does not exist or is not paused.
   */
  async resumeGroup(groupId: string): Promise<boolean> {
    const removed = await this.client.srem(`${this.keyPrefix}:groups:paused`, groupId);
    if (removed === 0) return false;
    // Re-add to active list so the scheduler picks it up again.
    const inSet = await this.client.sismember(`${this.keyPrefix}:groups:set`, groupId);
    if (inSet) {
      await this.client.rpush(`${this.keyPrefix}:groups:active`, groupId);
    }
    await this.xadd('group-resumed', { groupId });
    return true;
  }

  /**
   * Set a local (per-group) concurrency override.
   * Takes precedence over WorkerOptions.group.concurrency.
   */
  async setGroupConcurrency(groupId: string, concurrency: number): Promise<void> {
    await this.client.set(`${this.keyPrefix}:group:cfg:${groupId}`, String(concurrency));
  }

  /**
   * Get the locally configured concurrency for a group.
   * Returns undefined if no local override has been set.
   */
  async getGroupConcurrency(groupId: string): Promise<number | undefined> {
    const val = await this.client.get(`${this.keyPrefix}:group:cfg:${groupId}`);
    return val !== null ? parseInt(val, 10) : undefined;
  }

  /**
   * Get the remaining rate-limit TTL (ms) for a group.
   * Returns 0 if the group is not currently rate limited.
   */
  async getGroupRateLimitTtl(groupId: string): Promise<number> {
    const ttl = await this.client.pttl(`${this.keyPrefix}:group:rate-limit:${groupId}`);
    return Math.max(0, ttl);
  }

  /**
   * Get job counts per intra-group priority level.
   * priority=0 counts FIFO (waiting) jobs; priority>0 counts prioritized jobs.
   *
   * @example
   * const counts = await queue.getCountsPerPriorityForGroup('groupId', [1, 0]);
   * // { '1': 11, '0': 10 }
   */
  async getCountsPerPriorityForGroup(groupId: string, priorities: number[]): Promise<Record<string, number>> {
    const result: Record<string, number> = {};
    for (const priority of priorities) {
      if (priority === 0) {
        result['0'] = await this.client.llen(`${this.keyPrefix}:group:${groupId}`);
      } else {
        result[String(priority)] = await this.client.zcount(
          `${this.keyPrefix}:group:priority:${groupId}`,
          priority,
          priority,
        );
      }
    }
    return result;
  }

  // ─── Misc / admin ────────────────────────────────────────────────────────────

  async updateJobProgress(jobId: string, progress: number | object): Promise<void> {
    await this.client.hset(`${this.keyPrefix}:job:${jobId}`, 'progress', JSON.stringify(progress));
    await this.xadd('progress', { jobId, data: progress });
    this.emit('progress', jobId, progress);
  }

  async addJobLog(jobId: string, logRow: string, keepLogs?: number): Promise<number> {
    const job = await this.getJob(jobId);
    if (!job) return 0;
    return job.log(logRow);
  }

  async getRepeatableJobs(start = 0, end = -1, asc = false): Promise<unknown[]> {
    return [];
  }

  async trimEvents(maxLength: number): Promise<number> {
    return this.client.xtrim(`${this.keyPrefix}:events`, 'MAXLEN', maxLength);
  }

  async rateLimit(expireTimeMs: number): Promise<void> {
    await this.client.set(`${this.keyPrefix}:rate-limit`, '1', 'PX', expireTimeMs);
  }

  async setGlobalConcurrency(concurrency: number): Promise<number> {
    return this.client.set(`${this.keyPrefix}:concurrency`, String(concurrency)).then(() => concurrency);
  }

  async removeGlobalConcurrency(): Promise<number> {
    return this.client.del(`${this.keyPrefix}:concurrency`);
  }

  async setGlobalRateLimit(max: number, duration: number): Promise<number> {
    await this.client.hset(`${this.keyPrefix}:rate-config`, 'max', String(max), 'duration', String(duration));
    return max;
  }

  async removeGlobalRateLimit(): Promise<number> {
    return this.client.del(`${this.keyPrefix}:rate-config`);
  }

  private async xadd(event: string, data: object): Promise<void> {
    const maxLen = this.opts.streams?.events?.maxLen ?? 10000;
    await this.client.xadd(
      `${this.keyPrefix}:events`,
      'MAXLEN', '~', String(maxLen),
      '*',
      'event', event,
      'data', JSON.stringify(data),
    );
  }

  async close(): Promise<void> {
    await this.client.quit();
  }
}
