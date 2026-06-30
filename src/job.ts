import type Redis from 'ioredis';
import type { JobsOptions, JobJson, JobState, FinishedStatus, JobProgress } from './types';

export class Job<
  DataType = unknown,
  ReturnType = unknown,
  NameType extends string = string,
> {
  id: string;
  name: NameType;
  data: DataType;
  opts: JobsOptions;
  progress: JobProgress = 0;
  attemptsMade = 0;
  attemptsStarted = 0;
  stalledCounter = 0;
  timestamp: number;
  processedOn?: number;
  finishedOn?: number;
  returnvalue: ReturnType | null = null;
  failedReason = '';
  stacktrace: string[] = [];
  repeatJobKey?: string;
  parentKey?: string;
  parent?: { id: string; queueKey: string };
  deduplicationId?: string;
  token?: string;
  processedBy?: string;

  readonly queueName: string;
  readonly prefix: string;

  constructor(
    private readonly client: Redis,
    queueName: string,
    prefix: string,
    name: NameType,
    data: DataType,
    opts: JobsOptions = {},
    id?: string,
  ) {
    this.queueName = queueName;
    this.prefix = prefix;
    this.name = name;
    this.data = data;
    this.opts = opts;
    this.id = id ?? opts.jobId ?? Job.generateId();
    this.timestamp = opts.timestamp ?? Date.now();
  }

  get queueQualifiedName(): string {
    return `${this.prefix}:${this.queueName}`;
  }

  get delay(): number {
    return this.opts.delay ?? 0;
  }

  get priority(): number {
    return this.opts.priority ?? 0;
  }

  private get jobKey(): string {
    return `${this.queueQualifiedName}:job:${this.id}`;
  }

  private static generateId(): string {
    return `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
  }

  async updateProgress(progress: JobProgress): Promise<void> {
    this.progress = progress;
    await this.client.hset(this.jobKey, 'progress', JSON.stringify(progress));
  }

  /** @alias updateProgress */
  async update(data: DataType): Promise<void> {
    this.data = data;
    await this.client.hset(this.jobKey, 'data', JSON.stringify(data));
  }

  async updateData(data: DataType): Promise<void> {
    return this.update(data);
  }

  async log(logRow: string): Promise<number> {
    const logsKey = `${this.queueQualifiedName}:logs:${this.id}`;
    const len = await this.client.rpush(logsKey, logRow);
    const keepLogs = this.opts.keepLogs;
    if (keepLogs && len > keepLogs) {
      await this.client.ltrim(logsKey, -keepLogs, -1);
    }
    return len;
  }

  async clearLogs(keepLogs?: number): Promise<void> {
    const logsKey = `${this.queueQualifiedName}:logs:${this.id}`;
    if (keepLogs) {
      await this.client.ltrim(logsKey, -keepLogs, -1);
    } else {
      await this.client.del(logsKey);
    }
  }

  async remove(): Promise<void> {
    await this.client.del(this.jobKey);
    await this.client.del(`${this.queueQualifiedName}:logs:${this.id}`);
  }

  async extendLock(token: string, duration: number): Promise<number> {
    const lockKey = `${this.queueQualifiedName}:processing:${this.id}`;
    const current = await this.client.get(lockKey);
    if (current !== token) return 0;
    return this.client.pexpire(lockKey, duration);
  }

  async moveToCompleted(
    returnValue: ReturnType,
    token: string,
    fetchNext = false,
  ): Promise<void> {
    this.returnvalue = returnValue;
    this.finishedOn = Date.now();
    await this.save();
    await this.client.zadd(`${this.queueQualifiedName}:completed`, this.finishedOn, this.id);
    await this.client.del(`${this.queueQualifiedName}:processing:${this.id}`);
    await this.client.lrem(`${this.queueQualifiedName}:active`, 1, this.id);
  }

  async moveToFailed<E extends Error>(err: E, token: string, fetchNext = false): Promise<void> {
    this.failedReason = err.message;
    this.finishedOn = Date.now();
    if (err.stack) this.stacktrace.push(err.stack);
    await this.save();
    await this.client.zadd(`${this.queueQualifiedName}:failed`, this.finishedOn, this.id);
    await this.client.del(`${this.queueQualifiedName}:processing:${this.id}`);
    await this.client.lrem(`${this.queueQualifiedName}:active`, 1, this.id);
  }

  async moveToDelayed(timestamp: number, token?: string): Promise<void> {
    await this.client.zadd(`${this.queueQualifiedName}:delayed`, timestamp, this.id);
  }

  async moveToWait(token?: string): Promise<number> {
    return this.client.rpush(`${this.queueQualifiedName}:ready`, this.id);
  }

  async promote(): Promise<void> {
    const score = await this.client.zscore(`${this.queueQualifiedName}:delayed`, this.id);
    if (score === null) return;
    await this.client.zrem(`${this.queueQualifiedName}:delayed`, this.id);
    if (this.opts.group?.id) {
      await this.client.rpush(`${this.queueQualifiedName}:group:${this.opts.group.id}`, this.id);
      const added = await this.client.sadd(`${this.queueQualifiedName}:groups:set`, this.opts.group.id);
      if (added) await this.client.rpush(`${this.queueQualifiedName}:groups:active`, this.opts.group.id);
    } else if (this.priority > 0) {
      await this.client.zadd(`${this.queueQualifiedName}:priority`, this.priority, this.id);
    } else {
      await this.client.rpush(`${this.queueQualifiedName}:ready`, this.id);
    }
  }

  async retry(state: FinishedStatus = 'failed'): Promise<void> {
    const setKey = `${this.queueQualifiedName}:${state}`;
    await this.client.zrem(setKey, this.id);
    this.attemptsMade = 0;
    this.failedReason = '';
    this.finishedOn = undefined;
    await this.save();
    if (this.opts.group?.id) {
      await this.client.rpush(`${this.queueQualifiedName}:group:${this.opts.group.id}`, this.id);
      const added = await this.client.sadd(`${this.queueQualifiedName}:groups:set`, this.opts.group.id);
      if (added) await this.client.rpush(`${this.queueQualifiedName}:groups:active`, this.opts.group.id);
    } else if (this.priority > 0) {
      await this.client.zadd(`${this.queueQualifiedName}:priority`, this.priority, this.id);
    } else {
      await this.client.rpush(`${this.queueQualifiedName}:ready`, this.id);
    }
  }

  async getState(): Promise<JobState> {
    const qn = this.queueQualifiedName;
    if (await this.client.exists(`${qn}:processing:${this.id}`)) return 'active';
    if ((await this.client.zscore(`${qn}:delayed`, this.id)) !== null) return 'delayed';
    if ((await this.client.zscore(`${qn}:failed`, this.id)) !== null) return 'failed';
    if ((await this.client.zscore(`${qn}:completed`, this.id)) !== null) return 'completed';
    const inReady = await this.client.lpos(`${qn}:ready`, this.id);
    if (inReady !== null) return 'waiting';
    const isPaused = await this.client.exists(`${qn}:paused`);
    if (isPaused) return 'paused';
    return 'unknown';
  }

  async isCompleted(): Promise<boolean> {
    return (await this.client.zscore(`${this.queueQualifiedName}:completed`, this.id)) !== null;
  }

  async isFailed(): Promise<boolean> {
    return (await this.client.zscore(`${this.queueQualifiedName}:failed`, this.id)) !== null;
  }

  async isDelayed(): Promise<boolean> {
    return (await this.client.zscore(`${this.queueQualifiedName}:delayed`, this.id)) !== null;
  }

  async isActive(): Promise<boolean> {
    return (await this.client.exists(`${this.queueQualifiedName}:processing:${this.id}`)) === 1;
  }

  async isWaiting(): Promise<boolean> {
    return (await this.client.lpos(`${this.queueQualifiedName}:ready`, this.id)) !== null;
  }

  async isWaitingChildren(): Promise<boolean> {
    return false;
  }

  async changeDelay(delay: number): Promise<void> {
    this.opts.delay = delay;
    const newTimestamp = Date.now() + delay;
    await this.client.zadd(`${this.queueQualifiedName}:delayed`, newTimestamp, this.id);
    await this.save();
  }

  async changePriority(opts: { priority?: number; lifo?: boolean }): Promise<void> {
    if (opts.priority !== undefined) {
      this.opts.priority = opts.priority;
      await this.save();
      if (opts.priority > 0) {
        await this.client.lrem(`${this.queueQualifiedName}:ready`, 1, this.id);
        await this.client.zadd(`${this.queueQualifiedName}:priority`, opts.priority, this.id);
      }
    }
  }

  async removeDeduplicationKey(): Promise<boolean> {
    const id = this.deduplicationId ?? this.opts.deduplication?.id;
    if (!id) return false;
    const deleted = await this.client.del(`${this.queueQualifiedName}:dedup:${id}`);
    return deleted > 0;
  }

  toJSON(): Record<string, unknown> {
    return {
      id: this.id,
      name: this.name,
      data: this.data,
      opts: this.opts,
      progress: this.progress,
      attemptsMade: this.attemptsMade,
      attemptsStarted: this.attemptsStarted,
      stalledCounter: this.stalledCounter,
      timestamp: this.timestamp,
      processedOn: this.processedOn,
      finishedOn: this.finishedOn,
      returnvalue: this.returnvalue,
      failedReason: this.failedReason,
      stacktrace: this.stacktrace,
      parentKey: this.parentKey,
      repeatJobKey: this.repeatJobKey,
    };
  }

  async save(): Promise<void> {
    await this.client.hset(this.jobKey, {
      id: this.id,
      name: this.name,
      data: JSON.stringify(this.data),
      opts: JSON.stringify(this.opts),
      progress: JSON.stringify(this.progress),
      attemptsMade: String(this.attemptsMade),
      attemptsStarted: String(this.attemptsStarted),
      stalledCounter: String(this.stalledCounter),
      timestamp: String(this.timestamp),
      processedOn: this.processedOn ? String(this.processedOn) : '',
      finishedOn: this.finishedOn ? String(this.finishedOn) : '',
      returnvalue: JSON.stringify(this.returnvalue),
      failedReason: this.failedReason,
      stacktrace: JSON.stringify(this.stacktrace),
    });
  }

  static async create<T, R, N extends string = string>(
    client: Redis,
    queueName: string,
    prefix: string,
    name: N,
    data: T,
    opts?: JobsOptions,
  ): Promise<Job<T, R, N>> {
    const job = new Job<T, R, N>(client, queueName, prefix, name, data, opts);
    await job.save();
    return job;
  }

  static async fromId<T = unknown, R = unknown, N extends string = string>(
    client: Redis,
    queueName: string,
    prefix: string,
    jobId: string,
  ): Promise<Job<T, R, N> | null> {
    const key = `${prefix}:${queueName}:job:${jobId}`;
    const data = await client.hgetall(key);
    if (!data || !data.id) return null;
    return Job.fromJSON<T, R, N>(client, queueName, prefix, data as unknown as JobJson);
  }

  static fromJSON<T = unknown, R = unknown, N extends string = string>(
    client: Redis,
    queueName: string,
    prefix: string,
    json: JobJson,
  ): Job<T, R, N> {
    const opts: JobsOptions = json.opts ? JSON.parse(json.opts) : {};
    const job = new Job<T, R, N>(
      client,
      queueName,
      prefix,
      json.name as N,
      json.data ? JSON.parse(json.data) as T : undefined as T,
      opts,
      json.id,
    );
    job.progress = json.progress ? JSON.parse(json.progress) : 0;
    job.attemptsMade = Number(json.attemptsMade ?? 0);
    job.attemptsStarted = Number(json.attemptsStarted ?? 0);
    job.stalledCounter = Number(json.stalledCounter ?? 0);
    job.timestamp = Number(json.timestamp ?? Date.now());
    job.processedOn = json.processedOn ? Number(json.processedOn) : undefined;
    job.finishedOn = json.finishedOn ? Number(json.finishedOn) : undefined;
    job.returnvalue = json.returnvalue && json.returnvalue !== 'null'
      ? JSON.parse(json.returnvalue) as R
      : null;
    job.failedReason = json.failedReason ?? '';
    job.stacktrace = json.stacktrace ? JSON.parse(json.stacktrace) : [];
    return job;
  }
}
