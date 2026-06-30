import { EventEmitter } from 'events';
import type Redis from 'ioredis';
import type { QueueOptions, JobsOptions, JobCounts, JobState, DefaultJobOptions } from './types';
import { Job } from './job';
export declare class Queue<DataType = unknown, ResultType = unknown, NameType extends string = string> extends EventEmitter {
    readonly name: string;
    readonly opts: Required<Pick<QueueOptions, 'prefix'>> & QueueOptions;
    readonly defaultJobOptions: DefaultJobOptions;
    private client;
    constructor(name: string, opts?: QueueOptions);
    get keyPrefix(): string;
    waitUntilReady(): Promise<Redis>;
    add(name: NameType, data: DataType, opts?: JobsOptions): Promise<Job<DataType, ResultType, NameType>>;
    addBulk(jobs: Array<{
        name: NameType;
        data: DataType;
        opts?: JobsOptions;
    }>): Promise<Job<DataType, ResultType, NameType>[]>;
    private enqueue;
    getJob(jobId: string): Promise<Job<DataType, ResultType, NameType> | null>;
    getJobs(types: JobState | JobState[], start?: number, end?: number, asc?: boolean): Promise<Job<DataType, ResultType, NameType>[]>;
    private getJobIdsByType;
    getJobCounts(...types: JobState[]): Promise<JobCounts>;
    getActiveCount(): Promise<number>;
    getWaitingCount(): Promise<number>;
    getDelayedCount(): Promise<number>;
    getCompletedCount(): Promise<number>;
    getFailedCount(): Promise<number>;
    pause(): Promise<void>;
    resume(): Promise<void>;
    isPaused(): Promise<boolean>;
    drain(delayed?: boolean): Promise<void>;
    obliterate(opts?: {
        force?: boolean;
        count?: number;
    }): Promise<void>;
    clean(grace: number, limit: number, type?: 'completed' | 'failed' | 'active' | 'wait' | 'delayed' | 'paused'): Promise<string[]>;
    remove(jobId: string, opts?: {
        removeChildren?: boolean;
    }): Promise<number>;
    retryJobs(opts?: {
        count?: number;
        state?: 'completed' | 'failed';
        timestamp?: number;
    }): Promise<void>;
    promoteJobs(opts?: {
        count?: number;
    }): Promise<void>;
    updateJobProgress(jobId: string, progress: number | object): Promise<void>;
    addJobLog(jobId: string, logRow: string, keepLogs?: number): Promise<number>;
    getRepeatableJobs(start?: number, end?: number, asc?: boolean): Promise<unknown[]>;
    trimEvents(maxLength: number): Promise<number>;
    rateLimit(expireTimeMs: number): Promise<void>;
    setGlobalConcurrency(concurrency: number): Promise<number>;
    removeGlobalConcurrency(): Promise<number>;
    setGlobalRateLimit(max: number, duration: number): Promise<number>;
    removeGlobalRateLimit(): Promise<number>;
    private xadd;
    close(): Promise<void>;
}
//# sourceMappingURL=queue.d.ts.map