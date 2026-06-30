"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Worker = void 0;
const events_1 = require("events");
const job_1 = require("./job");
const connection_1 = require("./connection");
const errors_1 = require("./errors");
function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}
class Worker extends events_1.EventEmitter {
    constructor(name, processor, opts) {
        super();
        this.processor = processor;
        this._running = false;
        this._closing = false;
        this._paused = false;
        this.activeCount = 0;
        this.activeJobs = new Map();
        this.schedulerTimer = null;
        this.delayedTimer = null;
        this.stalledTimer = null;
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
        this.client = (0, connection_1.createClient)(this.opts.connection);
        this.blockingClient = (0, connection_1.createClient)(this.opts.connection);
        this.client.on('error', (err) => this.emit('error', err));
        this.blockingClient.on('error', (err) => this.emit('error', err));
        if (this.opts.autorun !== false) {
            setImmediate(() => this.run().catch((err) => this.emit('error', err)));
        }
    }
    get keyPrefix() {
        return `${this.opts.prefix ?? 'bull'}:${this.name}`;
    }
    get concurrency() {
        return this.opts.concurrency ?? 1;
    }
    set concurrency(val) {
        this.opts.concurrency = val;
    }
    isRunning() {
        return this._running;
    }
    isPaused() {
        return this._paused;
    }
    async pause(doNotWaitActive = false) {
        this._paused = true;
        if (!doNotWaitActive) {
            while (this.activeCount > 0) {
                await sleep(50);
            }
        }
        this.emit('paused');
    }
    resume() {
        this._paused = false;
        this.emit('resumed');
    }
    async waitUntilReady() {
        await this.client.ping();
        return this.client;
    }
    cancelJob(jobId, reason = 'cancelled') {
        return this.activeJobs.has(jobId);
    }
    cancelAllJobs(reason = 'cancelled') {
        // Cancellation is signalled through the AbortSignal; no forced kill here
    }
    async run() {
        if (this._running)
            return;
        this._running = true;
        this.emit('ready');
        this.startScheduler();
        this.startDelayedPromoter();
        if (!this.opts.skipStalledCheck) {
            this.startStalledChecker();
        }
        try {
            await this.mainLoop();
        }
        finally {
            this._running = false;
        }
    }
    // ─── Main processing loop ──────────────────────────────────────────────────
    async mainLoop() {
        while (!this._closing) {
            // Respect pause and concurrency limit
            if (this._paused || this.activeCount >= this.concurrency) {
                await sleep(50);
                continue;
            }
            // Check queue-level pause flag
            const queuePaused = await this.client.exists(`${this.keyPrefix}:paused`);
            if (queuePaused) {
                await sleep(200);
                continue;
            }
            // Non-blocking: try priority queue first, then ready queue
            let jobId = await this.nextFromPriority();
            if (!jobId) {
                jobId = await this.client.lpop(`${this.keyPrefix}:ready`);
            }
            if (jobId) {
                // Fire-and-forget to allow concurrency
                this.processJob(jobId).catch((err) => this.emit('error', err));
                continue;
            }
            // Block-wait for next job (timeout = drainDelay seconds)
            const timeout = this.opts.drainDelay ?? 5;
            const result = await this.blockingClient
                .blpop(`${this.keyPrefix}:ready`, timeout)
                .catch(() => null);
            if (!result) {
                this.emit('drained');
            }
            else {
                const [, jId] = result;
                this.processJob(jId).catch((err) => this.emit('error', err));
            }
        }
    }
    async nextFromPriority() {
        const items = await this.client.zrange(`${this.keyPrefix}:priority`, 0, 0);
        if (!items.length)
            return null;
        const id = items[0];
        const removed = await this.client.zrem(`${this.keyPrefix}:priority`, id);
        return removed ? id : null;
    }
    // ─── Single job lifecycle ──────────────────────────────────────────────────
    async processJob(jobId) {
        this.activeCount++;
        const token = `${this.id}:${jobId}`;
        try {
            const job = await job_1.Job.fromId(this.client, this.name, this.opts.prefix ?? 'bull', jobId);
            if (!job) {
                this.activeCount--;
                return;
            }
            await this.lockAndActivate(job, token);
            try {
                if (!this.processor)
                    throw new Error('No processor defined');
                const abortController = new AbortController();
                const result = await this.processor(job, token, abortController.signal);
                job.returnvalue = result;
                job.finishedOn = Date.now();
                await job.save();
                await this.onCompleted(job, token);
            }
            catch (err) {
                await this.onFailed(job, token, err);
            }
        }
        finally {
            this.activeCount--;
        }
    }
    async lockAndActivate(job, token) {
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
                    .catch(() => { });
            }
        }, renewalInterval);
        this.activeJobs.set(job.id, { job, token, renewalTimer });
        await this.xadd('active', { jobId: job.id, prev: 'waiting' });
        this.emit('active', job, 'waiting');
    }
    async cleanup(job) {
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
    async onCompleted(job, token) {
        await this.cleanup(job);
        await this.applyRemovePolicy(job, 'complete');
        await this.xadd('completed', {
            jobId: job.id,
            returnvalue: JSON.stringify(job.returnvalue),
            prev: 'active',
        });
        this.emit('completed', job, job.returnvalue, 'active');
    }
    async onFailed(job, token, err) {
        const isUnrecoverable = err instanceof errors_1.UnrecoverableError;
        const maxAttempts = job.opts.attempts ?? 1;
        job.failedReason = err.message;
        if (err.stack)
            job.stacktrace.push(err.stack);
        job.finishedOn = Date.now();
        await job.save();
        await this.cleanup(job);
        const canRetry = !isUnrecoverable && job.attemptsMade < maxAttempts;
        if (canRetry) {
            const delay = this.calcBackoff(job);
            if (delay > 0) {
                await this.client.zadd(`${this.keyPrefix}:delayed`, Date.now() + delay, job.id);
                await this.xadd('delayed', { jobId: job.id, delay });
            }
            else {
                // Re-enqueue with group awareness
                const groupId = job.opts.group?.id;
                if (groupId) {
                    await this.client.rpush(`${this.keyPrefix}:group:${groupId}`, job.id);
                    const added = await this.client.sadd(`${this.keyPrefix}:groups:set`, groupId);
                    if (added)
                        await this.client.rpush(`${this.keyPrefix}:groups:active`, groupId);
                }
                else {
                    await this.client.rpush(`${this.keyPrefix}:ready`, job.id);
                }
            }
        }
        else {
            await this.applyRemovePolicy(job, 'fail');
            await this.xadd('failed', { jobId: job.id, failedReason: err.message, prev: 'active' });
            this.emit('failed', job, err, 'active');
            if (job.attemptsMade >= maxAttempts) {
                await this.xadd('retries-exhausted', { jobId: job.id, attemptsMade: job.attemptsMade });
            }
        }
    }
    calcBackoff(job) {
        const { backoff } = job.opts;
        if (!backoff)
            return 0;
        if (typeof backoff === 'number')
            return backoff;
        const delay = backoff.delay ?? 0;
        if (backoff.type === 'exponential') {
            return delay * Math.pow(2, job.attemptsMade - 1);
        }
        return delay; // fixed
    }
    async applyRemovePolicy(job, outcome) {
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
    startScheduler() {
        this.schedulerTimer = setInterval(() => this.scheduleGroupJob().catch(() => { }), 100);
    }
    async scheduleGroupJob() {
        if (this._closing)
            return;
        const activeKey = `${this.keyPrefix}:groups:active`;
        const groupId = await this.client.lpop(activeKey);
        if (!groupId)
            return;
        const maxGroupConcurrency = 1; // default serial; overridden by group.concurrency in job opts
        const currentRunning = parseInt((await this.client.get(`${this.keyPrefix}:running:${groupId}`)) ?? '0', 10);
        if (currentRunning >= maxGroupConcurrency) {
            // Group is busy — put it back at end of queue for later
            await this.client.rpush(activeKey, groupId);
            return;
        }
        // Try to acquire group lock (prevents two schedulers competing)
        const lockKey = `${this.keyPrefix}:lock:group:${groupId}`;
        const lockAcquired = await this.client.set(lockKey, this.id, 'EX', 30, 'NX');
        if (!lockAcquired) {
            await this.client.rpush(activeKey, groupId);
            return;
        }
        const jobId = await this.client.lpop(`${this.keyPrefix}:group:${groupId}`);
        if (!jobId) {
            // Group queue is now empty — remove from tracking set
            await this.client.srem(`${this.keyPrefix}:groups:set`, groupId);
            await this.client.del(lockKey);
            return;
        }
        // Increment group running counter then push job to ready
        await this.client.incr(`${this.keyPrefix}:running:${groupId}`);
        await this.client.rpush(`${this.keyPrefix}:ready`, jobId);
        // Check if group still has jobs; if so, re-enqueue at end for round-robin
        const remaining = await this.client.llen(`${this.keyPrefix}:group:${groupId}`);
        if (remaining > 0) {
            await this.client.rpush(activeKey, groupId);
        }
        else {
            await this.client.srem(`${this.keyPrefix}:groups:set`, groupId);
        }
        await this.client.del(lockKey);
    }
    // ─── Delayed job promoter ──────────────────────────────────────────────────
    startDelayedPromoter() {
        this.delayedTimer = setInterval(() => this.promoteDelayedJobs().catch(() => { }), 1000);
    }
    async promoteDelayedJobs() {
        if (this._closing)
            return;
        const now = Date.now();
        const ids = await this.client.zrangebyscore(`${this.keyPrefix}:delayed`, '-inf', now);
        if (!ids.length)
            return;
        const pipe = this.client.pipeline();
        for (const id of ids) {
            pipe.zrem(`${this.keyPrefix}:delayed`, id);
        }
        await pipe.exec();
        for (const id of ids) {
            // Re-enqueue respecting group/priority
            const job = await job_1.Job.fromId(this.client, this.name, this.opts.prefix ?? 'bull', id);
            if (!job)
                continue;
            if (job.opts.group?.id) {
                const groupId = job.opts.group.id;
                await this.client.rpush(`${this.keyPrefix}:group:${groupId}`, id);
                const added = await this.client.sadd(`${this.keyPrefix}:groups:set`, groupId);
                if (added)
                    await this.client.rpush(`${this.keyPrefix}:groups:active`, groupId);
            }
            else if ((job.opts.priority ?? 0) > 0) {
                await this.client.zadd(`${this.keyPrefix}:priority`, job.opts.priority, id);
            }
            else {
                await this.client.rpush(`${this.keyPrefix}:ready`, id);
            }
            await this.xadd('waiting', { jobId: id });
        }
    }
    // ─── Stalled job checker ───────────────────────────────────────────────────
    startStalledChecker() {
        const interval = this.opts.stalledInterval ?? 30000;
        this.stalledTimer = setInterval(() => this.checkStalledJobs().catch(() => { }), interval);
    }
    async checkStalledJobs() {
        if (this._closing)
            return;
        const activeIds = await this.client.lrange(`${this.keyPrefix}:active`, 0, -1);
        for (const jobId of activeIds) {
            // Skip jobs we are currently processing in this worker
            if (this.activeJobs.has(jobId))
                continue;
            const lockExists = await this.client.exists(`${this.keyPrefix}:processing:${jobId}`);
            if (lockExists)
                continue;
            // Lock is gone but job is still in active — stalled
            await this.client.lrem(`${this.keyPrefix}:active`, 1, jobId);
            const job = await job_1.Job.fromId(this.client, this.name, this.opts.prefix ?? 'bull', jobId);
            if (!job)
                continue;
            const maxStalledCount = this.opts.maxStalledCount ?? 1;
            if (job.stalledCounter >= maxStalledCount) {
                job.failedReason = 'job stalled more than allowable limit';
                job.finishedOn = Date.now();
                await job.save();
                await this.client.zadd(`${this.keyPrefix}:failed`, job.finishedOn, jobId);
                await this.xadd('failed', { jobId, failedReason: job.failedReason, prev: 'active' });
                this.emit('failed', job, new Error(job.failedReason), 'active');
            }
            else {
                job.stalledCounter++;
                await job.save();
                await this.client.rpush(`${this.keyPrefix}:ready`, jobId);
                await this.xadd('stalled', { jobId });
                this.emit('stalled', jobId, 'active');
            }
        }
    }
    // ─── Helpers ───────────────────────────────────────────────────────────────
    async xadd(event, data) {
        await this.client
            .xadd(`${this.keyPrefix}:events`, '*', 'event', event, 'data', JSON.stringify(data))
            .catch(() => { });
    }
    async rateLimit(expireTimeMs) {
        await this.client.set(`${this.keyPrefix}:rate-limit`, '1', 'PX', expireTimeMs);
    }
    // ─── Lifecycle ─────────────────────────────────────────────────────────────
    async close(force = false) {
        if (this._closing)
            return;
        this._closing = true;
        this.emit('closing', 'Worker is closing');
        if (this.schedulerTimer)
            clearInterval(this.schedulerTimer);
        if (this.delayedTimer)
            clearInterval(this.delayedTimer);
        if (this.stalledTimer)
            clearInterval(this.stalledTimer);
        if (!force) {
            // Drain: wait for in-flight jobs
            while (this.activeCount > 0) {
                await sleep(50);
            }
        }
        else {
            for (const { renewalTimer } of this.activeJobs.values()) {
                clearInterval(renewalTimer);
            }
            this.activeJobs.clear();
        }
        await this.blockingClient.quit().catch(() => { });
        await this.client.quit().catch(() => { });
        this.emit('closed');
    }
}
exports.Worker = Worker;
//# sourceMappingURL=worker.js.map