/**
 * Queue API surface tests — requires a live Redis on 127.0.0.1:6379
 * Run: pnpm test
 */
import { describe, it, expect, beforeEach } from 'vitest'
import Redis from 'ioredis'
import { Queue, Worker } from '../src'
import { GroupMaxSizeExceededError } from '../src/errors'

const connection = { host: '127.0.0.1', port: 6379 }

async function flush() {
  const r = new Redis(connection)
  await r.flushall()
  await r.quit()
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms))
}

// ─── Retrieval & counts ───────────────────────────────────────────────────────

describe('Job retrieval and counts', () => {
  beforeEach(flush)

  it('getJob hydrates a saved job and returns null for a missing id', async () => {
    const queue = new Queue<{ n: number }, void>('q-getjob', { connection })

    const job = await queue.add('typed', { n: 7 }, { attempts: 3 })
    const loaded = await queue.getJob(job.id)

    expect(loaded).not.toBeNull()
    expect(loaded!.id).toBe(job.id)
    expect(loaded!.name).toBe('typed')
    expect(loaded!.data).toEqual({ n: 7 })
    expect(loaded!.opts.attempts).toBe(3)

    expect(await queue.getJob('does-not-exist')).toBeNull()

    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('getJobCounts reflects waiting, delayed, completed and failed states', async () => {
    const queue = new Queue<{ i: number }, void>('q-counts', { connection })
    const keyPrefix = 'bull:q-counts'
    const client = new Redis(connection)

    await queue.add('a', { i: 0 })
    await queue.add('b', { i: 1 })
    await queue.add('c', { i: 2 }, { delay: 60000 })
    await client.zadd(`${keyPrefix}:completed`, Date.now(), 'done-1')
    await client.zadd(`${keyPrefix}:failed`, Date.now(), 'bad-1')

    const counts = await queue.getJobCounts()
    expect(counts).toEqual({
      active: 0, completed: 1, failed: 1, delayed: 1, waiting: 2, paused: 0,
    })

    expect(await queue.getWaitingCount()).toBe(2)
    expect(await queue.getDelayedCount()).toBe(1)
    expect(await queue.getCompletedCount()).toBe(1)
    expect(await queue.getFailedCount()).toBe(1)
    expect(await queue.getActiveCount()).toBe(0)

    await client.quit()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('getJobs returns hydrated jobs filtered by state', async () => {
    const queue = new Queue<{ i: number }, void>('q-getjobs', { connection })

    const j1 = await queue.add('a', { i: 0 })
    const j2 = await queue.add('b', { i: 1 })
    const j3 = await queue.add('c', { i: 2 }, { delay: 60000 })

    const waiting = await queue.getJobs('waiting')
    expect(waiting.map((j) => j.id).sort()).toEqual([j1.id, j2.id].sort())
    expect(waiting.find((j) => j.id === j1.id)!.data).toEqual({ i: 0 })

    const both = await queue.getJobs(['waiting', 'delayed'])
    expect(both).toHaveLength(3)
    expect(both.map((j) => j.id)).toContain(j3.id)

    await queue.obliterate({ force: true })
    await queue.close()
  })
})

// ─── Queue-level pause / resume ───────────────────────────────────────────────

describe('Queue pause / resume', () => {
  beforeEach(flush)

  it('a paused queue stops job pickup until resumed', async () => {
    const queue = new Queue<{ i: number }, void>('q-pause', { connection })

    await queue.pause()
    expect(await queue.isPaused()).toBe(true)

    await queue.addBulk(Array.from({ length: 3 }, (_, i) => ({ name: 't', data: { i } })))

    const processed: number[] = []
    const worker = new Worker<{ i: number }, void>(
      'q-pause',
      async (job) => { processed.push(job.data.i) },
      { connection, autorun: false, drainDelay: 1 },
    )
    worker.on('error', () => {})
    worker.run().catch(() => {})

    await sleep(400)
    expect(processed).toHaveLength(0)

    await queue.resume()
    expect(await queue.isPaused()).toBe(false)

    await new Promise<void>((resolve) => {
      worker.on('completed', () => { if (processed.length === 3) resolve() })
    })
    expect(processed).toHaveLength(3)

    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })
})

// ─── drain / clean / remove ───────────────────────────────────────────────────

describe('drain, clean and remove', () => {
  beforeEach(flush)

  it('drain clears waiting jobs, and delayed jobs only when asked', async () => {
    const queue = new Queue<{ i: number }, void>('q-drain', { connection })

    await queue.add('a', { i: 0 })
    await queue.add('b', { i: 1 })
    await queue.add('c', { i: 2 }, { delay: 60000 })

    await queue.drain()
    expect(await queue.getWaitingCount()).toBe(0)
    expect(await queue.getDelayedCount()).toBe(1)

    await queue.drain(true)
    expect(await queue.getDelayedCount()).toBe(0)

    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('clean removes finished jobs older than the grace period', async () => {
    const queue = new Queue<{ i: number }, void>('q-clean', { connection })
    const keyPrefix = 'bull:q-clean'
    const client = new Redis(connection)

    const oldJobs = await Promise.all(
      Array.from({ length: 3 }, (_, i) => queue.add('old', { i })),
    )
    const recentJob = await queue.add('recent', { i: 99 })

    for (const j of oldJobs) {
      await client.zadd(`${keyPrefix}:completed`, Date.now() - 60000, j.id)
    }
    await client.zadd(`${keyPrefix}:completed`, Date.now(), recentJob.id)

    const removed = await queue.clean(30000, 10)
    expect(removed.sort()).toEqual(oldJobs.map((j) => j.id).sort())

    expect(await client.zcard(`${keyPrefix}:completed`)).toBe(1)
    for (const j of oldJobs) {
      expect(await client.exists(`${keyPrefix}:job:${j.id}`)).toBe(0)
    }
    expect(await client.exists(`${keyPrefix}:job:${recentJob.id}`)).toBe(1)

    await client.quit()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('remove deletes a waiting non-group job and returns 0 for a missing one', async () => {
    const queue = new Queue<{ i: number }, void>('q-remove', { connection })
    const keyPrefix = 'bull:q-remove'
    const client = new Redis(connection)

    const job = await queue.add('t', { i: 0 })

    expect(await queue.remove(job.id)).toBe(1)
    expect(await client.lpos(`${keyPrefix}:ready`, job.id)).toBeNull()
    expect(await client.exists(`${keyPrefix}:job:${job.id}`)).toBe(0)

    expect(await queue.remove(job.id)).toBe(0)

    await client.quit()
    await queue.obliterate({ force: true })
    await queue.close()
  })
})

// ─── retryJobs / promoteJobs ──────────────────────────────────────────────────

describe('retryJobs and promoteJobs', () => {
  beforeEach(flush)

  it('retryJobs moves failed jobs back to waiting and resets attempts', async () => {
    const queue = new Queue<{ i: number }, void>('q-retryjobs', { connection })
    const keyPrefix = 'bull:q-retryjobs'
    const client = new Redis(connection)

    const worker = new Worker<{ i: number }, void>(
      'q-retryjobs',
      async () => { throw new Error('boom') },
      { connection, autorun: false, drainDelay: 1 },
    )
    worker.on('error', () => {})

    const job = await queue.add('t', { i: 0 })
    await new Promise<void>((resolve) => {
      worker.on('failed', () => resolve())
      worker.run().catch(() => {})
    })
    await worker.close()

    expect(await client.zscore(`${keyPrefix}:failed`, job.id)).not.toBeNull()

    await queue.retryJobs({ state: 'failed' })

    expect(await client.zscore(`${keyPrefix}:failed`, job.id)).toBeNull()
    expect(await client.lrange(`${keyPrefix}:ready`, 0, -1)).toContain(job.id)
    expect((await queue.getJob(job.id))!.attemptsMade).toBe(0)

    await client.quit()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('promoteJobs routes a due group job into its group queue, not straight to :ready', async () => {
    const queue = new Queue<{ i: number }, void>('q-promote-grp', { connection })
    const keyPrefix = 'bull:q-promote-grp'
    const client = new Redis(connection)

    const job = await queue.add('t', { i: 0 }, { delay: 1, group: { id: 'g' } })
    await sleep(10)

    await queue.promoteJobs()

    // Before the fix this bypassed group FIFO/concurrency entirely.
    expect(await client.lrange(`${keyPrefix}:ready`, 0, -1)).not.toContain(job.id)
    expect(await client.lrange(`${keyPrefix}:group:g`, 0, -1)).toContain(job.id)
    expect(await client.lrange(`${keyPrefix}:groups:active`, 0, -1)).toContain('g')

    await client.quit()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('promoteJobs defers a due group job instead of overflowing maxSize', async () => {
    const queue = new Queue<{ i: number }, void>('q-promote-max', { connection })
    const keyPrefix = 'bull:q-promote-max'
    const client = new Redis(connection)

    const delayed = await queue.add('t', { i: 0 }, { delay: 1, group: { id: 'g', maxSize: 1 } })
    await queue.add('t', { i: 1 }, { group: { id: 'g', maxSize: 1 } }) // fills the group
    await sleep(10)

    await queue.promoteJobs()

    expect(await client.lrange(`${keyPrefix}:ready`, 0, -1)).not.toContain(delayed.id)
    expect(await client.llen(`${keyPrefix}:group:g`)).toBe(1)
    // Deferred back onto :delayed for a later retry, not dropped.
    expect(await client.zscore(`${keyPrefix}:delayed`, delayed.id)).not.toBeNull()

    await client.quit()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('promoteJobs promotes only delayed jobs whose time has come', async () => {
    const queue = new Queue<{ i: number }, void>('q-promote', { connection })
    const keyPrefix = 'bull:q-promote'
    const client = new Redis(connection)

    const due = await queue.add('t', { i: 0 }, { delay: 1 })
    const future = await queue.add('t', { i: 1 }, { delay: 60000 })
    await sleep(10)

    await queue.promoteJobs()

    const ready = await client.lrange(`${keyPrefix}:ready`, 0, -1)
    expect(ready).toContain(due.id)
    expect(ready).not.toContain(future.id)
    expect(await client.zscore(`${keyPrefix}:delayed`, due.id)).toBeNull()
    expect(await client.zscore(`${keyPrefix}:delayed`, future.id)).not.toBeNull()

    await client.quit()
    await queue.obliterate({ force: true })
    await queue.close()
  })
})

// ─── Admission control ────────────────────────────────────────────────────────

describe('Group maxSize admission', () => {
  beforeEach(flush)

  it('a rejected add does not leave an orphan job hash behind', async () => {
    const queue = new Queue<{ i: number }, void>('q-orphan', { connection })
    const client = new Redis(connection)

    await queue.add('t', { i: 0 }, { group: { id: 'g', maxSize: 1 } })
    await expect(
      queue.add('t', { i: 1 }, { group: { id: 'g', maxSize: 1 } }),
    ).rejects.toBeInstanceOf(GroupMaxSizeExceededError)

    // Only the admitted job's hash may exist — the rejected one used to leak.
    const hashes = await client.keys('bull:q-orphan:job:*')
    expect(hashes).toHaveLength(1)

    await client.quit()
    await queue.obliterate({ force: true })
    await queue.close()
  })
})

// ─── Group helper getters ─────────────────────────────────────────────────────

describe('Group helper getters', () => {
  beforeEach(flush)

  it('getCountsPerPriorityForGroup counts FIFO and prioritized jobs separately', async () => {
    const queue = new Queue<{ i: number }, void>('q-grp-counts', { connection })

    await queue.add('t', { i: 0 }, { group: { id: 'g' } })
    await queue.add('t', { i: 1 }, { group: { id: 'g' } })
    await queue.add('t', { i: 2 }, { group: { id: 'g', priority: 1 } })
    await queue.add('t', { i: 3 }, { group: { id: 'g', priority: 1 } })
    await queue.add('t', { i: 4 }, { group: { id: 'g', priority: 1 } })

    const counts = await queue.getCountsPerPriorityForGroup('g', [0, 1])
    expect(counts).toEqual({ '0': 2, '1': 3 })

    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('getGroupRateLimitTtl returns 0 when unlimited and the remaining TTL while limited', async () => {
    const queue = new Queue<{ i: number }, void>('q-grp-ttl', { connection })

    expect(await queue.getGroupRateLimitTtl('g')).toBe(0)

    const job = await queue.add('t', { i: 0 }, { group: { id: 'g' } })
    const worker = new Worker<{ i: number }, void>(
      'q-grp-ttl',
      async () => {},
      { connection, autorun: false, drainDelay: 1 },
    )
    worker.on('error', () => {})
    await worker.rateLimitGroup(job, 5000)

    const ttl = await queue.getGroupRateLimitTtl('g')
    expect(ttl).toBeGreaterThan(0)
    expect(ttl).toBeLessThanOrEqual(5000)

    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })
})

// ─── Progress, logs, events stream, defaults ──────────────────────────────────

describe('Progress, logs and events stream', () => {
  beforeEach(flush)

  it('updateJobProgress persists progress and emits locally', async () => {
    const queue = new Queue<{ i: number }, void>('q-progress', { connection })
    const job = await queue.add('t', { i: 0 })

    let emitted: [string, unknown] | undefined
    queue.on('progress', (jobId: string, p: unknown) => { emitted = [jobId, p] })

    await queue.updateJobProgress(job.id, { pct: 50 })

    expect((await queue.getJob(job.id))!.progress).toEqual({ pct: 50 })
    expect(emitted).toEqual([job.id, { pct: 50 }])

    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('addJobLog appends to the job log and returns 0 for a missing job', async () => {
    const queue = new Queue<{ i: number }, void>('q-log', { connection })
    const job = await queue.add('t', { i: 0 })

    expect(await queue.addJobLog(job.id, 'hello')).toBe(1)
    expect(await queue.addJobLog(job.id, 'world')).toBe(2)
    expect(await queue.addJobLog('does-not-exist', 'x')).toBe(0)

    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('trimEvents caps the events stream length', async () => {
    const queue = new Queue<{ i: number }, void>('q-trim', { connection })
    const client = new Redis(connection)

    await queue.add('a', { i: 0 })
    await queue.add('b', { i: 1 })
    await queue.add('c', { i: 2 })
    expect(await client.xlen('bull:q-trim:events')).toBeGreaterThanOrEqual(3)

    await queue.trimEvents(1)
    expect(await client.xlen('bull:q-trim:events')).toBe(1)

    await client.quit()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('defaultJobOptions are merged into added jobs and overridable per add', async () => {
    const queue = new Queue<{ i: number }, void>('q-defaults', {
      connection,
      defaultJobOptions: { attempts: 5, removeOnComplete: true },
    })

    const a = await queue.add('t', { i: 0 })
    expect(a.opts.attempts).toBe(5)
    expect(a.opts.removeOnComplete).toBe(true)

    const b = await queue.add('t', { i: 1 }, { attempts: 2 })
    expect(b.opts.attempts).toBe(2)
    expect(b.opts.removeOnComplete).toBe(true)

    // Merged options must round-trip through Redis, not just live on the instance.
    expect((await queue.getJob(a.id))!.opts.attempts).toBe(5)

    await queue.obliterate({ force: true })
    await queue.close()
  })
})
