/**
 * Job class tests — requires a live Redis on 127.0.0.1:6379
 * Run: pnpm test
 */
import { describe, it, expect, beforeEach } from 'vitest'
import Redis from 'ioredis'
import { Queue, Worker } from '../src'

const connection = { host: '127.0.0.1', port: 6379 }

async function flush() {
  const r = new Redis(connection)
  await r.flushall()
  await r.quit()
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms))
}

// ─── State transitions ────────────────────────────────────────────────────────

describe('Job.getState', () => {
  beforeEach(flush)

  it('reports delayed, waiting, group-waiting and unknown states', async () => {
    const queue = new Queue<{ i: number }, void>('j-state', { connection })
    const keyPrefix = 'bull:j-state'
    const client = new Redis(connection)

    const delayed = await queue.add('t', { i: 0 }, { delay: 60000 })
    expect(await delayed.getState()).toBe('delayed')

    const waiting = await queue.add('t', { i: 1 })
    expect(await waiting.getState()).toBe('waiting')

    const grouped = await queue.add('t', { i: 2 }, { group: { id: 'g' } })
    expect(await grouped.getState()).toBe('waiting')

    const prioritizedGrouped = await queue.add('t', { i: 3 }, { group: { id: 'g', priority: 2 } })
    expect(await prioritizedGrouped.getState()).toBe('waiting')

    // Pull the plain job out of :ready without processing it — no structure
    // references it any more, so its state is unknown.
    await client.lrem(`${keyPrefix}:ready`, 0, waiting.id)
    expect(await waiting.getState()).toBe('unknown')

    await client.quit()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('reports active, completed and failed states through a real worker', async () => {
    const queue = new Queue<{ fail: boolean }, void>('j-state-live', { connection })

    const worker = new Worker<{ fail: boolean }, void>(
      'j-state-live',
      async (job) => {
        if (job.data.fail) throw new Error('nope')
        await sleep(150)
      },
      { connection, autorun: false, drainDelay: 1 },
    )
    worker.on('error', () => {})

    const okJob = await queue.add('t', { fail: false })
    const activeSeen = new Promise<void>((resolve) => worker.once('active', () => resolve()))
    const completedSeen = new Promise<void>((resolve) => worker.once('completed', () => resolve()))
    worker.run().catch(() => {})

    await activeSeen
    expect(await okJob.getState()).toBe('active')
    await completedSeen
    expect(await okJob.getState()).toBe('completed')

    const failedSeen = new Promise<void>((resolve) => worker.once('failed', () => resolve()))
    const failJob = await queue.add('t', { fail: true })
    await failedSeen
    expect(await failJob.getState()).toBe('failed')
    expect(await failJob.isFailed()).toBe(true)
    expect(await okJob.isCompleted()).toBe(true)

    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })
})

// ─── Serialization ────────────────────────────────────────────────────────────

describe('Job serialization', () => {
  beforeEach(flush)

  it('round-trips data, opts and mutable fields through Redis', async () => {
    const queue = new Queue<Record<string, unknown>, Record<string, unknown>>('j-roundtrip', { connection })

    const data = { nested: { arr: [1, 2, 3], s: 'x' }, flag: true }
    const job = await queue.add('rt', data, {
      attempts: 4,
      backoff: { type: 'exponential', delay: 250 },
      group: { id: 'g7', priority: 2 },
    })

    const loaded = await queue.getJob(job.id)
    expect(loaded!.data).toEqual(data)
    expect(loaded!.opts.attempts).toBe(4)
    expect(loaded!.opts.backoff).toEqual({ type: 'exponential', delay: 250 })
    expect(loaded!.opts.group).toEqual({ id: 'g7', priority: 2 })
    expect(loaded!.timestamp).toBe(job.timestamp)
    expect(loaded!.returnvalue).toBeNull()
    expect(loaded!.stacktrace).toEqual([])

    loaded!.returnvalue = { ok: 1 }
    loaded!.attemptsMade = 2
    loaded!.stacktrace.push('trace-line')
    await loaded!.save()

    const again = await queue.getJob(job.id)
    expect(again!.returnvalue).toEqual({ ok: 1 })
    expect(again!.attemptsMade).toBe(2)
    expect(again!.stacktrace).toEqual(['trace-line'])

    await queue.obliterate({ force: true })
    await queue.close()
  })
})

// ─── Mutations: progress, data, logs ──────────────────────────────────────────

describe('Job mutations', () => {
  beforeEach(flush)

  it('updateProgress and updateData persist to Redis', async () => {
    const queue = new Queue<{ n: number }, void>('j-mutate', { connection })
    const job = await queue.add('t', { n: 1 })

    await job.updateProgress(42)
    expect((await queue.getJob(job.id))!.progress).toBe(42)

    await job.updateProgress({ stage: 'half' })
    expect((await queue.getJob(job.id))!.progress).toEqual({ stage: 'half' })

    await job.updateData({ n: 99 })
    expect((await queue.getJob(job.id))!.data).toEqual({ n: 99 })

    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('log respects keepLogs trimming and clearLogs removes the log list', async () => {
    const queue = new Queue<{ i: number }, void>('j-logs', { connection })
    const keyPrefix = 'bull:j-logs'
    const client = new Redis(connection)

    const job = await queue.add('t', { i: 0 }, { keepLogs: 2 })

    await job.log('a')
    await job.log('b')
    const len = await job.log('c')
    expect(len).toBe(3) // length before trimming

    expect(await client.lrange(`${keyPrefix}:logs:${job.id}`, 0, -1)).toEqual(['b', 'c'])

    await job.clearLogs()
    expect(await client.exists(`${keyPrefix}:logs:${job.id}`)).toBe(0)

    await client.quit()
    await queue.obliterate({ force: true })
    await queue.close()
  })
})

// ─── Scheduling mutations: delay, priority, promote, retry ────────────────────

describe('Job scheduling mutations', () => {
  beforeEach(flush)

  it('changeDelay reschedules a delayed job', async () => {
    const queue = new Queue<{ i: number }, void>('j-delay', { connection })
    const keyPrefix = 'bull:j-delay'
    const client = new Redis(connection)

    const job = await queue.add('t', { i: 0 }, { delay: 60000 })

    await job.changeDelay(120000)

    const score = Number(await client.zscore(`${keyPrefix}:delayed`, job.id))
    expect(score).toBeGreaterThan(Date.now() + 100000)
    expect((await queue.getJob(job.id))!.opts.delay).toBe(120000)

    await client.quit()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('changePriority moves a waiting job into the priority zset', async () => {
    const queue = new Queue<{ i: number }, void>('j-priority', { connection })
    const keyPrefix = 'bull:j-priority'
    const client = new Redis(connection)

    const job = await queue.add('t', { i: 0 })

    await job.changePriority({ priority: 7 })

    expect(await client.lpos(`${keyPrefix}:ready`, job.id)).toBeNull()
    expect(await client.zscore(`${keyPrefix}:priority`, job.id)).toBe('7')
    expect((await queue.getJob(job.id))!.opts.priority).toBe(7)

    await client.quit()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('promote() moves a delayed group job into its group queue', async () => {
    const queue = new Queue<{ i: number }, void>('j-promote-grp', { connection })
    const keyPrefix = 'bull:j-promote-grp'
    const client = new Redis(connection)

    const job = await queue.add('t', { i: 0 }, { delay: 60000, group: { id: 'g' } })

    await job.promote()

    expect(await client.zscore(`${keyPrefix}:delayed`, job.id)).toBeNull()
    expect(await client.lrange(`${keyPrefix}:group:g`, 0, -1)).toContain(job.id)
    expect(await client.lrange(`${keyPrefix}:groups:active`, 0, -1)).toContain('g')

    await client.quit()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('promote() moves a delayed prioritized job into the priority zset', async () => {
    const queue = new Queue<{ i: number }, void>('j-promote-pri', { connection })
    const keyPrefix = 'bull:j-promote-pri'
    const client = new Redis(connection)

    const job = await queue.add('t', { i: 0 }, { delay: 60000, priority: 3 })

    await job.promote()

    expect(await client.zscore(`${keyPrefix}:delayed`, job.id)).toBeNull()
    expect(await client.zscore(`${keyPrefix}:priority`, job.id)).toBe('3')

    await client.quit()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('concurrent promote() calls enqueue the job exactly once', async () => {
    const queue = new Queue<{ i: number }, void>('j-promote-race', { connection })
    const keyPrefix = 'bull:j-promote-race'
    const client = new Redis(connection)

    const job = await queue.add('t', { i: 0 }, { delay: 60000 })
    const j1 = (await queue.getJob(job.id))!
    const j2 = (await queue.getJob(job.id))!

    // Two admin processes racing on the same delayed job — only the caller
    // whose ZREM claims it may enqueue.
    await Promise.all([j1.promote(), j2.promote()])

    const ready = await client.lrange(`${keyPrefix}:ready`, 0, -1)
    expect(ready.filter((id) => id === job.id)).toHaveLength(1)

    await client.quit()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('promote() is a no-op for a job that is not delayed', async () => {
    const queue = new Queue<{ i: number }, void>('j-promote-noop', { connection })
    const keyPrefix = 'bull:j-promote-noop'
    const client = new Redis(connection)

    const job = await queue.add('t', { i: 0 })

    await job.promote()

    // Still exactly one entry in :ready — the job was not double-enqueued.
    expect(await client.lrange(`${keyPrefix}:ready`, 0, -1)).toEqual([job.id])

    await client.quit()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('retry() re-enqueues a failed group job and resets its attempts', async () => {
    const queue = new Queue<{ i: number }, void>('j-retry-grp', { connection })
    const keyPrefix = 'bull:j-retry-grp'
    const client = new Redis(connection)

    const worker = new Worker<{ i: number }, void>(
      'j-retry-grp',
      async () => { throw new Error('boom') },
      { connection, autorun: false, drainDelay: 1, group: { concurrency: 1 } },
    )
    worker.on('error', () => {})

    const job = await queue.add('t', { i: 0 }, { group: { id: 'g' } })
    await new Promise<void>((resolve) => {
      worker.on('failed', () => resolve())
      worker.run().catch(() => {})
    })
    await worker.close()

    expect(await client.zscore(`${keyPrefix}:failed`, job.id)).not.toBeNull()

    const loaded = await queue.getJob(job.id)
    await loaded!.retry()

    expect(await client.zscore(`${keyPrefix}:failed`, job.id)).toBeNull()
    expect(await client.lrange(`${keyPrefix}:group:g`, 0, -1)).toContain(job.id)
    expect(await client.lrange(`${keyPrefix}:groups:active`, 0, -1)).toContain('g')
    expect((await queue.getJob(job.id))!.attemptsMade).toBe(0)

    await client.quit()
    await queue.obliterate({ force: true })
    await queue.close()
  })
})

// ─── Locking ──────────────────────────────────────────────────────────────────

describe('Job.extendLock', () => {
  beforeEach(flush)

  it('extends the processing lock only when the token matches', async () => {
    const queue = new Queue<{ i: number }, void>('j-lock', { connection })
    const keyPrefix = 'bull:j-lock'
    const client = new Redis(connection)

    const job = await queue.add('t', { i: 0 })
    await client.set(`${keyPrefix}:processing:${job.id}`, 'the-token', 'PX', 10000)

    expect(await job.extendLock('wrong-token', 60000)).toBe(0)
    expect(await job.extendLock('the-token', 60000)).toBe(1)
    expect(await client.pttl(`${keyPrefix}:processing:${job.id}`)).toBeGreaterThan(10000)

    await client.quit()
    await queue.obliterate({ force: true })
    await queue.close()
  })
})
