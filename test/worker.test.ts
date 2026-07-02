/**
 * Worker behavior tests — requires a live Redis on 127.0.0.1:6379
 * Run: pnpm test
 */
import { describe, it, expect, beforeEach } from 'vitest'
import Redis from 'ioredis'
import { Queue, Worker } from '../src'
import { UnrecoverableError } from '../src/errors'
import { scripted } from '../src/scripts'
import type { JobsOptions } from '../src/types'

const connection = { host: '127.0.0.1', port: 6379 }

async function flush() {
  const r = new Redis(connection)
  await r.flushall()
  await r.quit()
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms))
}

// ─── Standalone (non-group) priority ─────────────────────────────────────────

describe('Standalone job priority', () => {
  beforeEach(flush)

  it('processes prioritized jobs before FIFO jobs, lowest score first', async () => {
    const queue = new Queue<{ label: string }, void>('w-priority', { connection })

    await queue.add('t', { label: 'fifo' })
    await queue.add('t', { label: 'p10' }, { priority: 10 })
    await queue.add('t', { label: 'p1' }, { priority: 1 })
    await queue.add('t', { label: 'p5' }, { priority: 5 })

    const order: string[] = []
    const worker = new Worker<{ label: string }, void>(
      'w-priority',
      async (job) => { order.push(job.data.label) },
      { connection, autorun: false, drainDelay: 1, concurrency: 1 },
    )
    worker.on('error', () => {})

    await new Promise<void>((resolve) => {
      worker.on('completed', () => { if (order.length === 4) resolve() })
      worker.run().catch(() => {})
    })

    expect(order).toEqual(['p1', 'p5', 'p10', 'fifo'])

    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })
})

// ─── Delayed jobs end-to-end ──────────────────────────────────────────────────

describe('Delayed jobs', () => {
  beforeEach(flush)

  it('holds a delayed job until its delay elapses, then processes it', async () => {
    const queue = new Queue<{ i: number }, void>('w-delay', { connection })

    const t0 = Date.now()
    const job = await queue.add('t', { i: 0 }, { delay: 300 })
    expect(await job.getState()).toBe('delayed')

    let completedAt = 0
    const worker = new Worker<{ i: number }, void>(
      'w-delay',
      async () => { completedAt = Date.now() },
      { connection, autorun: false, drainDelay: 1 },
    )
    worker.on('error', () => {})

    await new Promise<void>((resolve) => {
      worker.on('completed', () => resolve())
      worker.run().catch(() => {})
    })

    expect(completedAt - t0).toBeGreaterThanOrEqual(300)

    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })
})

// ─── Retry semantics ──────────────────────────────────────────────────────────

describe('Retry semantics', () => {
  beforeEach(flush)

  it('UnrecoverableError fails the job immediately without consuming retries', async () => {
    const queue = new Queue<{ i: number }, void>('w-unrecoverable', { connection })
    const keyPrefix = 'bull:w-unrecoverable'
    const client = new Redis(connection)

    let attempts = 0
    const worker = new Worker<{ i: number }, void>(
      'w-unrecoverable',
      async () => {
        attempts++
        throw new UnrecoverableError('fatal')
      },
      { connection, autorun: false, drainDelay: 1 },
    )
    worker.on('error', () => {})

    const job = await queue.add('t', { i: 0 }, { attempts: 5 })

    let failedErr: Error | undefined
    await new Promise<void>((resolve) => {
      worker.on('failed', (_j, err) => { failedErr = err; resolve() })
      worker.run().catch(() => {})
    })

    expect(attempts).toBe(1)
    expect(failedErr!.name).toBe('UnrecoverableError')
    expect(await client.zscore(`${keyPrefix}:failed`, job.id)).not.toBeNull()
    expect(await client.zscore(`${keyPrefix}:delayed`, job.id)).toBeNull()

    await client.quit()
    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('calcBackoff computes fixed, numeric and exponential delays', async () => {
    const worker = new Worker<{ i: number }, void>(
      'w-backoff-calc',
      async () => {},
      { connection, autorun: false, drainDelay: 1 },
    )
    worker.on('error', () => {})
    const calc = (opts: JobsOptions, attemptsMade: number) =>
      (worker as unknown as {
        calcBackoff(job: { opts: JobsOptions; attemptsMade: number }): number
      }).calcBackoff({ opts, attemptsMade })

    expect(calc({}, 1)).toBe(0)
    expect(calc({ backoff: 500 }, 1)).toBe(500)
    expect(calc({ backoff: { type: 'fixed', delay: 250 } }, 3)).toBe(250)
    expect(calc({ backoff: { type: 'exponential', delay: 100 } }, 1)).toBe(100)
    expect(calc({ backoff: { type: 'exponential', delay: 100 } }, 2)).toBe(200)
    expect(calc({ backoff: { type: 'exponential', delay: 100 } }, 3)).toBe(400)
    expect(calc({ backoff: { type: 'exponential' } }, 3)).toBe(0)

    await worker.close()
  })

  it('waits the backoff delay before retrying a failed job', async () => {
    const queue = new Queue<{ i: number }, void>('w-backoff-e2e', { connection })

    let attempts = 0
    const worker = new Worker<{ i: number }, void>(
      'w-backoff-e2e',
      async () => {
        attempts++
        throw new Error('always fails')
      },
      { connection, autorun: false, drainDelay: 1 },
    )
    worker.on('error', () => {})

    const t0 = Date.now()
    await queue.add('t', { i: 0 }, {
      attempts: 2,
      backoff: { type: 'exponential', delay: 300 },
    })

    // 'failed' only fires on the final attempt — the first failure goes to :delayed.
    await new Promise<void>((resolve) => {
      worker.on('failed', () => resolve())
      worker.run().catch(() => {})
    })

    expect(attempts).toBe(2)
    expect(Date.now() - t0).toBeGreaterThanOrEqual(300)

    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })
})

// ─── Remove-on-finish policies ────────────────────────────────────────────────

describe('removeOnComplete / removeOnFail', () => {
  beforeEach(flush)

  it('removeOnComplete: true deletes the job entirely on success', async () => {
    const queue = new Queue<{ i: number }, void>('w-roc-true', { connection })
    const keyPrefix = 'bull:w-roc-true'
    const client = new Redis(connection)

    const job = await queue.add('t', { i: 0 }, { removeOnComplete: true })

    const worker = new Worker<{ i: number }, void>(
      'w-roc-true',
      async () => {},
      { connection, autorun: false, drainDelay: 1 },
    )
    worker.on('error', () => {})

    await new Promise<void>((resolve) => {
      worker.on('completed', () => resolve())
      worker.run().catch(() => {})
    })

    expect(await client.exists(`${keyPrefix}:job:${job.id}`)).toBe(0)
    expect(await client.zcard(`${keyPrefix}:completed`)).toBe(0)

    await client.quit()
    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('removeOnComplete: N keeps only the N most recent completed jobs', async () => {
    const queue = new Queue<{ i: number }, void>('w-roc-count', { connection })
    const keyPrefix = 'bull:w-roc-count'
    const client = new Redis(connection)

    const jobs = []
    for (let i = 0; i < 5; i++) {
      jobs.push(await queue.add('t', { i }, { removeOnComplete: 2 }))
    }

    let done = 0
    const worker = new Worker<{ i: number }, void>(
      'w-roc-count',
      // Small sleep so each finishedOn timestamp is distinct — the keep-count
      // trim orders by score.
      async () => { await sleep(5) },
      { connection, autorun: false, drainDelay: 1, concurrency: 1 },
    )
    worker.on('error', () => {})

    await new Promise<void>((resolve) => {
      worker.on('completed', () => { if (++done === 5) resolve() })
      worker.run().catch(() => {})
    })

    expect(await client.zcard(`${keyPrefix}:completed`)).toBe(2)
    let remainingHashes = 0
    for (const j of jobs) {
      if (await client.exists(`${keyPrefix}:job:${j.id}`)) remainingHashes++
    }
    expect(remainingHashes).toBe(2)

    await client.quit()
    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('removeOnFail: true deletes the job entirely on permanent failure', async () => {
    const queue = new Queue<{ i: number }, void>('w-rof-true', { connection })
    const keyPrefix = 'bull:w-rof-true'
    const client = new Redis(connection)

    const job = await queue.add('t', { i: 0 }, { removeOnFail: true })

    const worker = new Worker<{ i: number }, void>(
      'w-rof-true',
      async () => { throw new Error('boom') },
      { connection, autorun: false, drainDelay: 1 },
    )
    worker.on('error', () => {})

    await new Promise<void>((resolve) => {
      worker.on('failed', () => resolve())
      worker.run().catch(() => {})
    })

    expect(await client.exists(`${keyPrefix}:job:${job.id}`)).toBe(0)
    expect(await client.zcard(`${keyPrefix}:failed`)).toBe(0)

    await client.quit()
    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })
})

// ─── Worker-level pause / lifecycle ───────────────────────────────────────────

describe('Worker pause / lifecycle', () => {
  beforeEach(flush)

  it('a paused worker stops picking up jobs until resumed', async () => {
    const queue = new Queue<{ i: number }, void>('w-pause', { connection })

    const processed: number[] = []
    const worker = new Worker<{ i: number }, void>(
      'w-pause',
      async (job) => { processed.push(job.data.i) },
      { connection, autorun: false, drainDelay: 1 },
    )
    worker.on('error', () => {})

    // Pause before run() so the main loop parks on the pause check instead of BLPOP.
    await worker.pause()
    expect(worker.isPaused()).toBe(true)
    worker.run().catch(() => {})

    await queue.add('t', { i: 0 })
    await sleep(300)
    expect(processed).toHaveLength(0)

    worker.resume()
    expect(worker.isPaused()).toBe(false)
    await new Promise<void>((resolve) => worker.on('completed', () => resolve()))
    expect(processed).toEqual([0])

    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it("emits 'drained' when the queue runs empty", async () => {
    const queue = new Queue<{ i: number }, void>('w-drained', { connection })
    const worker = new Worker<{ i: number }, void>(
      'w-drained',
      async () => {},
      { connection, autorun: false, drainDelay: 1 },
    )
    worker.on('error', () => {})

    await new Promise<void>((resolve) => {
      worker.on('drained', () => resolve())
      worker.run().catch(() => {})
    })

    await worker.close()
    await queue.close()
  })

  it('close() waits for the in-flight job to finish', async () => {
    const queue = new Queue<{ i: number }, void>('w-close', { connection })

    let completed = 0
    const worker = new Worker<{ i: number }, void>(
      'w-close',
      async () => { await sleep(300) },
      { connection, autorun: false, drainDelay: 1 },
    )
    worker.on('error', () => {})
    worker.on('completed', () => { completed++ })

    await queue.add('t', { i: 0 })
    const active = new Promise<void>((resolve) => worker.once('active', () => resolve()))
    worker.run().catch(() => {})
    await active

    await worker.close()
    expect(completed).toBe(1)

    await queue.obliterate({ force: true })
    await queue.close()
  })
})

// ─── Stalled job recovery ─────────────────────────────────────────────────────

describe('Stalled job recovery', () => {
  beforeEach(flush)

  it('re-enqueues a stalled job and increments its stalledCounter', async () => {
    const queue = new Queue<{ i: number }, void>('w-stall', { connection })
    const keyPrefix = 'bull:w-stall'
    const client = new Redis(connection)

    const job = await queue.add('t', { i: 0 })

    const worker = new Worker<{ i: number }, void>(
      'w-stall',
      async () => {},
      { connection, autorun: false, drainDelay: 1 },
    )
    worker.on('error', () => {})
    const stalledIds: string[] = []
    worker.on('stalled', (jobId: string) => stalledIds.push(jobId))
    const w = worker as unknown as { checkStalledJobs(): Promise<void> }

    // Simulate a worker that picked the job up (moved it to :active) and died
    // before setting or renewing its processing lock.
    expect(await client.lpop(`${keyPrefix}:ready`)).toBe(job.id)
    await client.rpush(`${keyPrefix}:active`, job.id)

    // First scan only marks the lockless job as a suspect (grace pass, so a
    // freshly picked-up job isn't false-stalled); the second scan reclaims.
    await w.checkStalledJobs()
    expect(await client.lrange(`${keyPrefix}:active`, 0, -1)).toContain(job.id)
    await w.checkStalledJobs()

    expect(await client.lrange(`${keyPrefix}:active`, 0, -1)).not.toContain(job.id)
    expect(await client.lrange(`${keyPrefix}:ready`, 0, -1)).toContain(job.id)
    expect(stalledIds).toEqual([job.id])
    expect((await queue.getJob(job.id))!.stalledCounter).toBe(1)

    await client.quit()
    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('fails a job permanently once stalledCounter reaches maxStalledCount', async () => {
    const queue = new Queue<{ i: number }, void>('w-stall-fail', { connection })
    const keyPrefix = 'bull:w-stall-fail'
    const client = new Redis(connection)

    const job = await queue.add('t', { i: 0 })

    const worker = new Worker<{ i: number }, void>(
      'w-stall-fail',
      async () => {},
      { connection, autorun: false, drainDelay: 1, maxStalledCount: 1 },
    )
    worker.on('error', () => {})
    let failedReason = ''
    worker.on('failed', (_j, err) => { failedReason = err.message })
    const w = worker as unknown as { checkStalledJobs(): Promise<void> }

    // First stall: re-enqueued with stalledCounter = 1. (Each stall takes two
    // scans: the grace pass marks the suspect, the next one reclaims.)
    await client.lpop(`${keyPrefix}:ready`)
    await client.rpush(`${keyPrefix}:active`, job.id)
    await w.checkStalledJobs()
    await w.checkStalledJobs()
    expect(await client.lrange(`${keyPrefix}:ready`, 0, -1)).toContain(job.id)

    // Second stall: counter has reached the limit — permanent failure.
    await client.lpop(`${keyPrefix}:ready`)
    await client.rpush(`${keyPrefix}:active`, job.id)
    await w.checkStalledJobs()
    await w.checkStalledJobs()

    expect(await client.zscore(`${keyPrefix}:failed`, job.id)).not.toBeNull()
    expect(await client.lrange(`${keyPrefix}:ready`, 0, -1)).not.toContain(job.id)
    expect(failedReason).toBe('job stalled more than allowable limit')
    expect((await queue.getJob(job.id))!.failedReason).toBe('job stalled more than allowable limit')

    await client.quit()
    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('releases the group slot and re-queues a stalled group job', async () => {
    const queue = new Queue<{ i: number }, void>('w-stall-grp', { connection })
    const keyPrefix = 'bull:w-stall-grp'
    const client = new Redis(connection)

    const job = await queue.add('t', { i: 0 }, { group: { id: 'g' } })

    const worker = new Worker<{ i: number }, void>(
      'w-stall-grp',
      async () => {},
      { connection, autorun: false, drainDelay: 1, group: { concurrency: 1 } },
    )
    worker.on('error', () => {})
    const w = worker as unknown as {
      scheduleOneGroup(groupId: string): Promise<boolean>
      checkStalledJobs(): Promise<void>
    }

    expect(await w.scheduleOneGroup('g')).toBe(true)
    expect(await client.get(`${keyPrefix}:running:g`)).toBe('1')

    // The dispatched job is picked up (moved to :active) by a worker that dies
    // without a processing lock.
    expect(await client.lpop(`${keyPrefix}:ready`)).toBe(job.id)
    await client.rpush(`${keyPrefix}:active`, job.id)

    // Grace pass marks the suspect; the second scan reclaims.
    await w.checkStalledJobs()
    await w.checkStalledJobs()

    // Slot released, ownership map cleared, job back at the front of its group.
    expect(await client.get(`${keyPrefix}:running:g`)).toBe('0')
    expect(await client.hget(`${keyPrefix}:group:job-map`, job.id)).toBeNull()
    expect(await client.lrange(`${keyPrefix}:group:g`, 0, -1)).toContain(job.id)
    expect(await client.lrange(`${keyPrefix}:groups:active`, 0, -1)).toContain('g')

    await client.quit()
    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })
})

// ─── Pickup and dispatch invariants ───────────────────────────────────────────

describe('Pickup and dispatch invariants', () => {
  beforeEach(flush)

  it('releases the concurrency slot exactly once when the job hash is missing', async () => {
    const worker = new Worker<{ i: number }, void>(
      'w-slot-once',
      async () => {},
      { connection, autorun: false, drainDelay: 1 },
    )
    worker.on('error', () => {})
    const w = worker as unknown as { processJob(jobId: string): Promise<void>; activeCount: number }

    // mainLoop increments activeCount before calling processJob.
    w.activeCount = 1
    await w.processJob('no-such-job')

    // Before the fix the missing-hash branch released the slot AND the finally
    // block released it again, driving activeCount negative and silently
    // raising the effective concurrency.
    expect(w.activeCount).toBe(0)

    await worker.close()
  })

  it('remove() during processing neither resurrects the hash nor records completion', async () => {
    const queue = new Queue<{ i: number }, void>('w-remove-mid', { connection })
    const keyPrefix = 'bull:w-remove-mid'
    const client = new Redis(connection)

    let completedEvents = 0
    const worker = new Worker<{ i: number }, void>(
      'w-remove-mid',
      async () => { await sleep(300) },
      { connection, autorun: false, drainDelay: 1 },
    )
    worker.on('error', () => {})
    worker.on('completed', () => { completedEvents++ })

    const job = await queue.add('t', { i: 0 })
    const active = new Promise<void>((resolve) => worker.once('active', () => resolve()))
    worker.run().catch(() => {})
    await active

    await queue.remove(job.id)
    await sleep(500) // let the in-flight processor finish

    // The worker's completion-time save must not re-create the deleted hash,
    // and the removed job must not surface as completed.
    expect(await client.exists(`${keyPrefix}:job:${job.id}`)).toBe(0)
    expect(await client.zscore(`${keyPrefix}:completed`, job.id)).toBeNull()
    expect(completedEvents).toBe(0)

    await client.quit()
    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('the dispatch script refuses to exceed the group concurrency ceiling', async () => {
    const client = new Redis(connection)
    const s = scripted(client)
    const keyPrefix = 'bull:w-ceiling'

    await client.rpush(`${keyPrefix}:group:g`, 'job-1')
    await client.set(`${keyPrefix}:running:g`, '2')

    // Even without any scheduler lock held, the script itself must refuse —
    // and must NOT pop the job while refusing.
    const jobId = await s.pmqPopDispatch(
      `${keyPrefix}:group:priority:g`, `${keyPrefix}:group:g`,
      `${keyPrefix}:running:g`, `${keyPrefix}:group:job-map`, `${keyPrefix}:ready`,
      'g', '2',
    )

    expect(jobId).toBeNull()
    expect(await client.get(`${keyPrefix}:running:g`)).toBe('2')
    expect(await client.lrange(`${keyPrefix}:group:g`, 0, -1)).toEqual(['job-1'])
    expect(await client.llen(`${keyPrefix}:ready`)).toBe(0)
    expect(await client.hget(`${keyPrefix}:group:job-map`, 'job-1')).toBeNull()

    await client.quit()
  })

  it('the dispatch script drops a duplicate copy of an in-flight job instead of double-counting it', async () => {
    const client = new Redis(connection)
    const s = scripted(client)
    const keyPrefix = 'bull:w-dup'

    // job-1 is already in flight (owns a job-map entry); a second copy of the
    // same id sits in the group queue (e.g. from a stalled-recovery race).
    await client.hset(`${keyPrefix}:group:job-map`, 'job-1', 'g')
    await client.set(`${keyPrefix}:running:g`, '1')
    await client.rpush(`${keyPrefix}:group:g`, 'job-1')

    const jobId = await s.pmqPopDispatch(
      `${keyPrefix}:group:priority:g`, `${keyPrefix}:group:g`,
      `${keyPrefix}:running:g`, `${keyPrefix}:group:job-map`, `${keyPrefix}:ready`,
      'g', '5',
    )

    // The duplicate is consumed and dropped — not dispatched, not re-counted.
    expect(jobId).toBeNull()
    expect(await client.llen(`${keyPrefix}:group:g`)).toBe(0)
    expect(await client.llen(`${keyPrefix}:ready`)).toBe(0)
    expect(await client.get(`${keyPrefix}:running:g`)).toBe('1')
    expect(await client.hget(`${keyPrefix}:group:job-map`, 'job-1')).toBe('g')

    await client.quit()
  })

  it('priority pickup moves the job into :active atomically', async () => {
    const client = new Redis(connection)
    const s = scripted(client)
    const keyPrefix = 'bull:w-pickup-pri'

    await client.zadd(`${keyPrefix}:priority`, 5, 'job-1')

    const picked = await s.pmqPickupPriority(`${keyPrefix}:priority`, `${keyPrefix}:active`)

    // The job is never "nowhere": it leaves the zset and lands in :active in
    // one atomic step, where the stalled checker can always find it.
    expect(picked).toBe('job-1')
    expect(await client.zcard(`${keyPrefix}:priority`)).toBe(0)
    expect(await client.lrange(`${keyPrefix}:active`, 0, -1)).toContain('job-1')

    await client.quit()
  })
})

// ─── Worker-level group rate limit ────────────────────────────────────────────

describe('Worker-level group rate limit (group.limit)', () => {
  beforeEach(flush)

  it('dispatches at most `max` jobs per window, then rate-limits the group', async () => {
    const queue = new Queue<{ i: number }, void>('w-grp-limit', { connection })
    const keyPrefix = 'bull:w-grp-limit'
    const client = new Redis(connection)

    for (let i = 0; i < 4; i++) {
      await queue.add('t', { i }, { group: { id: 'g' } })
    }

    const worker = new Worker<{ i: number }, void>(
      'w-grp-limit',
      async () => {},
      {
        connection,
        autorun: false,
        drainDelay: 1,
        group: { concurrency: 5, limit: { max: 2, duration: 60000 } },
      },
    )
    worker.on('error', () => {})
    const w = worker as unknown as { scheduleOneGroup(groupId: string): Promise<boolean> }

    expect(await w.scheduleOneGroup('g')).toBe(true)

    // Only `max` jobs were dispatched despite 5 free concurrency slots.
    expect(await client.llen(`${keyPrefix}:ready`)).toBe(2)
    expect(await client.llen(`${keyPrefix}:group:g`)).toBe(2)
    expect(await client.get(`${keyPrefix}:running:g`)).toBe('2')

    // The group is now rate-limited for the remainder of the window...
    const ttl = await queue.getGroupRateLimitTtl('g')
    expect(ttl).toBeGreaterThan(0)
    expect(ttl).toBeLessThanOrEqual(60000)

    // ...so another scheduling pass dispatches nothing.
    expect(await w.scheduleOneGroup('g')).toBe(false)
    expect(await client.llen(`${keyPrefix}:ready`)).toBe(2)

    // The group stays in rotation to be retried once the window expires —
    // exactly once, no duplicate rotation entries piling up.
    const activeGroups = await client.lrange(`${keyPrefix}:groups:active`, 0, -1)
    expect(activeGroups.filter((g) => g === 'g')).toHaveLength(1)

    await client.quit()
    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })
})
