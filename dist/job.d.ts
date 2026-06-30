import type Redis from 'ioredis';
import type { JobsOptions, JobJson, JobState, FinishedStatus, JobProgress } from './types';
export declare class Job<DataType = unknown, ReturnType = unknown, NameType extends string = string> {
    private readonly client;
    id: string;
    name: NameType;
    data: DataType;
    opts: JobsOptions;
    progress: JobProgress;
    attemptsMade: number;
    attemptsStarted: number;
    stalledCounter: number;
    timestamp: number;
    processedOn?: number;
    finishedOn?: number;
    returnvalue: ReturnType | null;
    failedReason: string;
    stacktrace: string[];
    repeatJobKey?: string;
    parentKey?: string;
    parent?: {
        id: string;
        queueKey: string;
    };
    deduplicationId?: string;
    token?: string;
    processedBy?: string;
    readonly queueName: string;
    readonly prefix: string;
    constructor(client: Redis, queueName: string, prefix: string, name: NameType, data: DataType, opts?: JobsOptions, id?: string);
    get queueQualifiedName(): string;
    get delay(): number;
    get priority(): number;
    private get jobKey();
    private static generateId;
    updateProgress(progress: JobProgress): Promise<void>;
    /** @alias updateProgress */
    update(data: DataType): Promise<void>;
    updateData(data: DataType): Promise<void>;
    log(logRow: string): Promise<number>;
    clearLogs(keepLogs?: number): Promise<void>;
    remove(): Promise<void>;
    extendLock(token: string, duration: number): Promise<number>;
    moveToCompleted(returnValue: ReturnType, token: string, fetchNext?: boolean): Promise<void>;
    moveToFailed<E extends Error>(err: E, token: string, fetchNext?: boolean): Promise<void>;
    moveToDelayed(timestamp: number, token?: string): Promise<void>;
    moveToWait(token?: string): Promise<number>;
    promote(): Promise<void>;
    retry(state?: FinishedStatus): Promise<void>;
    getState(): Promise<JobState>;
    isCompleted(): Promise<boolean>;
    isFailed(): Promise<boolean>;
    isDelayed(): Promise<boolean>;
    isActive(): Promise<boolean>;
    isWaiting(): Promise<boolean>;
    isWaitingChildren(): Promise<boolean>;
    changeDelay(delay: number): Promise<void>;
    changePriority(opts: {
        priority?: number;
        lifo?: boolean;
    }): Promise<void>;
    removeDeduplicationKey(): Promise<boolean>;
    toJSON(): Record<string, unknown>;
    save(): Promise<void>;
    static create<T, R, N extends string = string>(client: Redis, queueName: string, prefix: string, name: N, data: T, opts?: JobsOptions): Promise<Job<T, R, N>>;
    static fromId<T = unknown, R = unknown, N extends string = string>(client: Redis, queueName: string, prefix: string, jobId: string): Promise<Job<T, R, N> | null>;
    static fromJSON<T = unknown, R = unknown, N extends string = string>(client: Redis, queueName: string, prefix: string, json: JobJson): Job<T, R, N>;
}
//# sourceMappingURL=job.d.ts.map