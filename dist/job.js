"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Job = void 0;
class Job {
    constructor(client, queueName, prefix, name, data, opts = {}, id) {
        this.client = client;
        this.progress = 0;
        this.attemptsMade = 0;
        this.attemptsStarted = 0;
        this.stalledCounter = 0;
        this.returnvalue = null;
        this.failedReason = '';
        this.stacktrace = [];
        this.queueName = queueName;
        this.prefix = prefix;
        this.name = name;
        this.data = data;
        this.opts = opts;
        this.id = id ?? opts.jobId ?? Job.generateId();
        this.timestamp = opts.timestamp ?? Date.now();
    }
    get queueQualifiedName() {
        return `${this.prefix}:${this.queueName}`;
    }
    get delay() {
        return this.opts.delay ?? 0;
    }
    get priority() {
        return this.opts.priority ?? 0;
    }
    get jobKey() {
        return `${this.queueQualifiedName}:job:${this.id}`;
    }
    static generateId() {
        return `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
    }
    async updateProgress(progress) {
        this.progress = progress;
        await this.client.hset(this.jobKey, 'progress', JSON.stringify(progress));
    }
    /** @alias updateProgress */
    async update(data) {
        this.data = data;
        await this.client.hset(this.jobKey, 'data', JSON.stringify(data));
    }
    async updateData(data) {
        return this.update(data);
    }
    async log(logRow) {
        const logsKey = `${this.queueQualifiedName}:logs:${this.id}`;
        const len = await this.client.rpush(logsKey, logRow);
        const keepLogs = this.opts.keepLogs;
        if (keepLogs && len > keepLogs) {
            await this.client.ltrim(logsKey, -keepLogs, -1);
        }
        return len;
    }
    async clearLogs(keepLogs) {
        const logsKey = `${this.queueQualifiedName}:logs:${this.id}`;
        if (keepLogs) {
            await this.client.ltrim(logsKey, -keepLogs, -1);
        }
        else {
            await this.client.del(logsKey);
        }
    }
    async remove() {
        await this.client.del(this.jobKey);
        await this.client.del(`${this.queueQualifiedName}:logs:${this.id}`);
    }
    async extendLock(token, duration) {
        const lockKey = `${this.queueQualifiedName}:processing:${this.id}`;
        const current = await this.client.get(lockKey);
        if (current !== token)
            return 0;
        return this.client.pexpire(lockKey, duration);
    }
    async moveToCompleted(returnValue, token, fetchNext = false) {
        this.returnvalue = returnValue;
        this.finishedOn = Date.now();
        await this.save();
        await this.client.zadd(`${this.queueQualifiedName}:completed`, this.finishedOn, this.id);
        await this.client.del(`${this.queueQualifiedName}:processing:${this.id}`);
        await this.client.lrem(`${this.queueQualifiedName}:active`, 1, this.id);
    }
    async moveToFailed(err, token, fetchNext = false) {
        this.failedReason = err.message;
        this.finishedOn = Date.now();
        if (err.stack)
            this.stacktrace.push(err.stack);
        await this.save();
        await this.client.zadd(`${this.queueQualifiedName}:failed`, this.finishedOn, this.id);
        await this.client.del(`${this.queueQualifiedName}:processing:${this.id}`);
        await this.client.lrem(`${this.queueQualifiedName}:active`, 1, this.id);
    }
    async moveToDelayed(timestamp, token) {
        await this.client.zadd(`${this.queueQualifiedName}:delayed`, timestamp, this.id);
    }
    async moveToWait(token) {
        return this.client.rpush(`${this.queueQualifiedName}:ready`, this.id);
    }
    async promote() {
        const score = await this.client.zscore(`${this.queueQualifiedName}:delayed`, this.id);
        if (score === null)
            return;
        await this.client.zrem(`${this.queueQualifiedName}:delayed`, this.id);
        if (this.opts.group?.id) {
            await this.client.rpush(`${this.queueQualifiedName}:group:${this.opts.group.id}`, this.id);
            const added = await this.client.sadd(`${this.queueQualifiedName}:groups:set`, this.opts.group.id);
            if (added)
                await this.client.rpush(`${this.queueQualifiedName}:groups:active`, this.opts.group.id);
        }
        else if (this.priority > 0) {
            await this.client.zadd(`${this.queueQualifiedName}:priority`, this.priority, this.id);
        }
        else {
            await this.client.rpush(`${this.queueQualifiedName}:ready`, this.id);
        }
    }
    async retry(state = 'failed') {
        const setKey = `${this.queueQualifiedName}:${state}`;
        await this.client.zrem(setKey, this.id);
        this.attemptsMade = 0;
        this.failedReason = '';
        this.finishedOn = undefined;
        await this.save();
        if (this.opts.group?.id) {
            await this.client.rpush(`${this.queueQualifiedName}:group:${this.opts.group.id}`, this.id);
            const added = await this.client.sadd(`${this.queueQualifiedName}:groups:set`, this.opts.group.id);
            if (added)
                await this.client.rpush(`${this.queueQualifiedName}:groups:active`, this.opts.group.id);
        }
        else if (this.priority > 0) {
            await this.client.zadd(`${this.queueQualifiedName}:priority`, this.priority, this.id);
        }
        else {
            await this.client.rpush(`${this.queueQualifiedName}:ready`, this.id);
        }
    }
    async getState() {
        const qn = this.queueQualifiedName;
        if (await this.client.exists(`${qn}:processing:${this.id}`))
            return 'active';
        if ((await this.client.zscore(`${qn}:delayed`, this.id)) !== null)
            return 'delayed';
        if ((await this.client.zscore(`${qn}:failed`, this.id)) !== null)
            return 'failed';
        if ((await this.client.zscore(`${qn}:completed`, this.id)) !== null)
            return 'completed';
        const inReady = await this.client.lpos(`${qn}:ready`, this.id);
        if (inReady !== null)
            return 'waiting';
        const isPaused = await this.client.exists(`${qn}:paused`);
        if (isPaused)
            return 'paused';
        return 'unknown';
    }
    async isCompleted() {
        return (await this.client.zscore(`${this.queueQualifiedName}:completed`, this.id)) !== null;
    }
    async isFailed() {
        return (await this.client.zscore(`${this.queueQualifiedName}:failed`, this.id)) !== null;
    }
    async isDelayed() {
        return (await this.client.zscore(`${this.queueQualifiedName}:delayed`, this.id)) !== null;
    }
    async isActive() {
        return (await this.client.exists(`${this.queueQualifiedName}:processing:${this.id}`)) === 1;
    }
    async isWaiting() {
        return (await this.client.lpos(`${this.queueQualifiedName}:ready`, this.id)) !== null;
    }
    async isWaitingChildren() {
        return false;
    }
    async changeDelay(delay) {
        this.opts.delay = delay;
        const newTimestamp = Date.now() + delay;
        await this.client.zadd(`${this.queueQualifiedName}:delayed`, newTimestamp, this.id);
        await this.save();
    }
    async changePriority(opts) {
        if (opts.priority !== undefined) {
            this.opts.priority = opts.priority;
            await this.save();
            if (opts.priority > 0) {
                await this.client.lrem(`${this.queueQualifiedName}:ready`, 1, this.id);
                await this.client.zadd(`${this.queueQualifiedName}:priority`, opts.priority, this.id);
            }
        }
    }
    async removeDeduplicationKey() {
        const id = this.deduplicationId ?? this.opts.deduplication?.id;
        if (!id)
            return false;
        const deleted = await this.client.del(`${this.queueQualifiedName}:dedup:${id}`);
        return deleted > 0;
    }
    toJSON() {
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
    async save() {
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
    static async create(client, queueName, prefix, name, data, opts) {
        const job = new Job(client, queueName, prefix, name, data, opts);
        await job.save();
        return job;
    }
    static async fromId(client, queueName, prefix, jobId) {
        const key = `${prefix}:${queueName}:job:${jobId}`;
        const data = await client.hgetall(key);
        if (!data || !data.id)
            return null;
        return Job.fromJSON(client, queueName, prefix, data);
    }
    static fromJSON(client, queueName, prefix, json) {
        const opts = json.opts ? JSON.parse(json.opts) : {};
        const job = new Job(client, queueName, prefix, json.name, json.data ? JSON.parse(json.data) : undefined, opts, json.id);
        job.progress = json.progress ? JSON.parse(json.progress) : 0;
        job.attemptsMade = Number(json.attemptsMade ?? 0);
        job.attemptsStarted = Number(json.attemptsStarted ?? 0);
        job.stalledCounter = Number(json.stalledCounter ?? 0);
        job.timestamp = Number(json.timestamp ?? Date.now());
        job.processedOn = json.processedOn ? Number(json.processedOn) : undefined;
        job.finishedOn = json.finishedOn ? Number(json.finishedOn) : undefined;
        job.returnvalue = json.returnvalue && json.returnvalue !== 'null'
            ? JSON.parse(json.returnvalue)
            : null;
        job.failedReason = json.failedReason ?? '';
        job.stacktrace = json.stacktrace ? JSON.parse(json.stacktrace) : [];
        return job;
    }
}
exports.Job = Job;
//# sourceMappingURL=job.js.map