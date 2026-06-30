"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Queue = void 0;
const events_1 = require("events");
const job_1 = require("./job");
const connection_1 = require("./connection");
class Queue extends events_1.EventEmitter {
    constructor(name, opts) {
        super();
        this.name = name;
        this.opts = {
            prefix: 'bull',
            defaultJobOptions: {},
            ...opts,
            connection: opts?.connection ?? { host: '127.0.0.1', port: 6379 },
        };
        this.defaultJobOptions = this.opts.defaultJobOptions ?? {};
        this.client = (0, connection_1.createClient)(this.opts.connection);
        this.client.on('error', (err) => this.emit('error', err));
    }
    get keyPrefix() {
        return `${this.opts.prefix}:${this.name}`;
    }
    async waitUntilReady() {
        await this.client.ping();
        return this.client;
    }
    async add(name, data, opts) {
        const jobOpts = { ...this.defaultJobOptions, ...opts };
        const job = new job_1.Job(this.client, this.name, this.opts.prefix, name, data, jobOpts);
        await job.save();
        await this.enqueue(job);
        await this.xadd('added', { jobId: job.id, name: job.name });
        this.emit('waiting', job);
        return job;
    }
    async addBulk(jobs) {
        return Promise.all(jobs.map((j) => this.add(j.name, j.data, j.opts)));
    }
    async enqueue(job) {
        const delay = job.opts.delay ?? 0;
        const groupId = job.opts.group?.id;
        const priority = job.opts.priority ?? 0;
        if (delay > 0) {
            await this.client.zadd(`${this.keyPrefix}:delayed`, Date.now() + delay, job.id);
        }
        else if (groupId) {
            await this.client.rpush(`${this.keyPrefix}:group:${groupId}`, job.id);
            // SADD returns 1 if newly added → only RPUSH to active list if not already there
            const added = await this.client.sadd(`${this.keyPrefix}:groups:set`, groupId);
            if (added === 1) {
                await this.client.rpush(`${this.keyPrefix}:groups:active`, groupId);
            }
        }
        else if (priority > 0) {
            await this.client.zadd(`${this.keyPrefix}:priority`, priority, job.id);
        }
        else {
            await this.client.rpush(`${this.keyPrefix}:ready`, job.id);
        }
    }
    async getJob(jobId) {
        return job_1.Job.fromId(this.client, this.name, this.opts.prefix, jobId);
    }
    async getJobs(types, start = 0, end = -1, asc = false) {
        const typeArr = Array.isArray(types) ? types : [types];
        const ids = [];
        for (const type of typeArr) {
            const typeIds = await this.getJobIdsByType(type, start, end, asc);
            ids.push(...typeIds);
        }
        const jobs = await Promise.all(ids.map((id) => this.getJob(id)));
        return jobs.filter((j) => j !== null);
    }
    async getJobIdsByType(type, start, end, asc) {
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
    async getJobCounts(...types) {
        const all = types.length === 0
            ? ['active', 'completed', 'failed', 'delayed', 'waiting', 'paused']
            : types;
        const counts = { active: 0, completed: 0, failed: 0, delayed: 0, waiting: 0, paused: 0 };
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
    async getActiveCount() {
        return this.client.llen(`${this.keyPrefix}:active`);
    }
    async getWaitingCount() {
        return this.client.llen(`${this.keyPrefix}:ready`);
    }
    async getDelayedCount() {
        return this.client.zcard(`${this.keyPrefix}:delayed`);
    }
    async getCompletedCount() {
        return this.client.zcard(`${this.keyPrefix}:completed`);
    }
    async getFailedCount() {
        return this.client.zcard(`${this.keyPrefix}:failed`);
    }
    async pause() {
        await this.client.set(`${this.keyPrefix}:paused`, '1');
        await this.xadd('paused', {});
        this.emit('paused');
    }
    async resume() {
        await this.client.del(`${this.keyPrefix}:paused`);
        await this.xadd('resumed', {});
        this.emit('resumed');
    }
    async isPaused() {
        return (await this.client.exists(`${this.keyPrefix}:paused`)) === 1;
    }
    async drain(delayed = false) {
        const pipe = this.client.pipeline();
        pipe.del(`${this.keyPrefix}:ready`);
        if (delayed)
            pipe.del(`${this.keyPrefix}:delayed`);
        await pipe.exec();
    }
    async obliterate(opts = {}) {
        let cursor = '0';
        do {
            const [next, keys] = await this.client.scan(cursor, 'MATCH', `${this.keyPrefix}:*`, 'COUNT', 100);
            cursor = next;
            if (keys.length > 0)
                await this.client.del(...keys);
        } while (cursor !== '0');
    }
    async clean(grace, limit, type = 'completed') {
        const redisType = type === 'wait' ? 'completed' : type;
        const cutoff = Date.now() - grace;
        const key = `${this.keyPrefix}:${redisType}`;
        const ids = await this.client.zrangebyscore(key, '-inf', cutoff, 'LIMIT', 0, limit);
        if (ids.length === 0)
            return ids;
        const pipe = this.client.pipeline();
        for (const id of ids) {
            pipe.zrem(key, id);
            pipe.del(`${this.keyPrefix}:job:${id}`);
        }
        await pipe.exec();
        this.emit('cleaned', ids, type);
        return ids;
    }
    async remove(jobId, opts = {}) {
        const job = await this.getJob(jobId);
        if (!job)
            return 0;
        await job.remove();
        // Remove from all state sets/lists
        const pipe = this.client.pipeline();
        pipe.zrem(`${this.keyPrefix}:completed`, jobId);
        pipe.zrem(`${this.keyPrefix}:failed`, jobId);
        pipe.zrem(`${this.keyPrefix}:delayed`, jobId);
        pipe.zrem(`${this.keyPrefix}:priority`, jobId);
        pipe.lrem(`${this.keyPrefix}:ready`, 0, jobId);
        pipe.lrem(`${this.keyPrefix}:active`, 0, jobId);
        await pipe.exec();
        await this.xadd('removed', { jobId, prev: 'unknown' });
        return 1;
    }
    async retryJobs(opts = {}) {
        const state = opts.state ?? 'failed';
        const count = opts.count ?? 1000;
        const key = `${this.keyPrefix}:${state}`;
        const ids = await this.client.zrange(key, 0, count - 1);
        await Promise.all(ids.map((id) => this.getJob(id).then((job) => job?.retry(state))));
    }
    async promoteJobs(opts = {}) {
        const count = opts.count ?? 1000;
        const now = Date.now();
        const ids = await this.client.zrangebyscore(`${this.keyPrefix}:delayed`, '-inf', now, 'LIMIT', 0, count);
        const pipe = this.client.pipeline();
        for (const id of ids) {
            pipe.zrem(`${this.keyPrefix}:delayed`, id);
            pipe.rpush(`${this.keyPrefix}:ready`, id);
        }
        await pipe.exec();
    }
    async updateJobProgress(jobId, progress) {
        await this.client.hset(`${this.keyPrefix}:job:${jobId}`, 'progress', JSON.stringify(progress));
        await this.xadd('progress', { jobId, data: progress });
        this.emit('progress', jobId, progress);
    }
    async addJobLog(jobId, logRow, keepLogs) {
        const job = await this.getJob(jobId);
        if (!job)
            return 0;
        return job.log(logRow);
    }
    async getRepeatableJobs(start = 0, end = -1, asc = false) {
        return [];
    }
    async trimEvents(maxLength) {
        return this.client.xtrim(`${this.keyPrefix}:events`, 'MAXLEN', maxLength);
    }
    async rateLimit(expireTimeMs) {
        await this.client.set(`${this.keyPrefix}:rate-limit`, '1', 'PX', expireTimeMs);
    }
    async setGlobalConcurrency(concurrency) {
        return this.client.set(`${this.keyPrefix}:concurrency`, String(concurrency)).then(() => concurrency);
    }
    async removeGlobalConcurrency() {
        return this.client.del(`${this.keyPrefix}:concurrency`);
    }
    async setGlobalRateLimit(max, duration) {
        await this.client.hset(`${this.keyPrefix}:rate-config`, 'max', String(max), 'duration', String(duration));
        return max;
    }
    async removeGlobalRateLimit() {
        return this.client.del(`${this.keyPrefix}:rate-config`);
    }
    async xadd(event, data) {
        const maxLen = this.opts.streams?.events?.maxLen ?? 10000;
        await this.client.xadd(`${this.keyPrefix}:events`, 'MAXLEN', '~', String(maxLen), '*', 'event', event, 'data', JSON.stringify(data));
    }
    async close() {
        await this.client.quit();
    }
}
exports.Queue = Queue;
//# sourceMappingURL=queue.js.map