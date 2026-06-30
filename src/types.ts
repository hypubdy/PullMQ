import type Redis from 'ioredis';

export type ConnectionOptions =
  | Redis
  | { host?: string; port?: number; password?: string; db?: number; tls?: object; url?: string };

export interface BackoffOptions {
  type: 'fixed' | 'exponential' | (string & {});
  delay?: number;
  jitter?: number;
}

export interface RepeatOptions {
  pattern?: string;
  every?: number;
  limit?: number;
  immediately?: boolean;
  count?: number;
  offset?: number;
  prevMillis?: number;
  jobId?: string;
  startDate?: Date | string | number;
  endDate?: Date | string | number;
  utc?: boolean;
  tz?: string;
  key?: string;
}

export interface ParentOptions {
  id: string;
  queue?: string;
}

export interface ParentKeys {
  id: string;
  queueKey: string;
}

export interface KeepJobs {
  count?: number;
  age?: number;
}

export interface DeduplicationOptions {
  id: string;
  ttl?: number;
}

export interface GroupOptions {
  id: string;
  /** Maximum number of jobs to queue in this group. Throws GroupMaxSizeExceededError when exceeded. */
  maxSize?: number;
  /** Intra-group priority (0–2,097,151). Higher number = lower priority. 0 = FIFO (default). */
  priority?: number;
}

export interface WorkerGroupOptions {
  /** Maximum number of jobs to process in parallel per group (global default). */
  concurrency?: number;
  /** Global rate limit applied to every group. */
  limit?: {
    max: number;
    duration: number; // milliseconds
  };
}

export interface DefaultJobOptions {
  timestamp?: number;
  priority?: number;
  delay?: number;
  attempts?: number;
  backoff?: number | BackoffOptions;
  lifo?: boolean;
  removeOnComplete?: boolean | number | KeepJobs;
  removeOnFail?: boolean | number | KeepJobs;
  keepLogs?: number;
  stackTraceLimit?: number;
  sizeLimit?: number;
}

export interface JobsOptions extends DefaultJobOptions {
  jobId?: string;
  repeat?: RepeatOptions;
  repeatJobKey?: string;
  parent?: ParentOptions;
  prevMillis?: number;
  deduplication?: DeduplicationOptions;
  /** @deprecated use deduplication */
  debounce?: DeduplicationOptions;
  failParentOnFailure?: boolean;
  ignoreDependencyOnFailure?: boolean;
  /** Group-based scheduling (BullMQ Pro compatible). */
  group?: GroupOptions;
}

export interface QueueOptions {
  connection: ConnectionOptions;
  prefix?: string;
  defaultJobOptions?: DefaultJobOptions;
  streams?: { events: { maxLen: number } };
  skipVersionCheck?: boolean;
}

export interface WorkerOptions {
  connection: ConnectionOptions;
  prefix?: string;
  concurrency?: number;
  autorun?: boolean;
  lockDuration?: number;
  lockRenewTime?: number;
  stalledInterval?: number;
  maxStalledCount?: number;
  drainDelay?: number;
  skipStalledCheck?: boolean;
  skipLockRenewal?: boolean;
  limiter?: { max: number; duration: number };
  removeOnComplete?: KeepJobs;
  removeOnFail?: KeepJobs;
  runRetryDelay?: number;
  /** Group-level controls (concurrency per group, global rate limit). */
  group?: WorkerGroupOptions;
}

export interface QueueEventsOptions {
  connection: ConnectionOptions;
  prefix?: string;
  lastEventId?: string;
  blockingTimeout?: number;
}

export interface JobJson {
  id: string;
  name: string;
  data: string;
  opts: string;
  progress: string;
  attemptsMade: string;
  attemptsStarted: string;
  stalledCounter: string;
  timestamp: string;
  processedOn: string;
  finishedOn: string;
  returnvalue: string;
  failedReason: string;
  stacktrace: string;
}

export interface JobCounts {
  active: number;
  completed: number;
  failed: number;
  delayed: number;
  waiting: number;
  paused: number;
  prioritized?: number;
}

export type JobState =
  | 'completed'
  | 'failed'
  | 'active'
  | 'delayed'
  | 'waiting'
  | 'paused'
  | 'waiting-children'
  | 'prioritized'
  | 'unknown';

export type FinishedStatus = 'completed' | 'failed';

export type JobProgress = number | object;

export type Processor<T = unknown, R = unknown, N extends string = string> = (
  job: import('./job').Job<T, R, N>,
  token?: string,
  signal?: AbortSignal,
) => Promise<R>;
