/**
 * PullMQ integration tests — requires a live Redis on 127.0.0.1:6379
 * Run: pnpm test
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import Redis from 'ioredis'
import { Queue, Worker } from '../src'
import { GroupMaxSizeExceededError, GroupRateLimitError } from '../src/errors'

const connection = { host: '127.0.0.1', port: 6379 }

async function flush() {
  const r = new Redis(connection)
  await r.flushall()
  await r.quit()
}

// ─── Basic queue / worker ─────────────────────────────────────────────────────

describe('Basic queue + worker', () => {
  let queue: Queue<{ n: number }, number>
  let worker: Worker<{ n: number }, number>

  beforeEach(async () => {
    await flush()
    queue = new Queue('basic', { connection })
    worker = new Worker<{ n: number }, number>(
      'basic',
      async (job) => job.data.n * 2,
      { connection, autorun: false, drainDelay: 1 },
    )
    worker.on('error', () => {})
  })

  afterEach(async () => {
    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('processes a job and returns a result', async () => {
    const job = await queue.add('double', { n: 21 })
    let result: number | undefined

    await new Promise<void>((resolve) => {
      worker.on('completed', (j, r) => { result = r as number; resolve() })
      worker.run().catch(() => {})
    })

    expect(result).toBe(42)
    expect(job.id).toBeTruthy()
  })

  it('processes multiple jobs in order', async () => {
    const TOTAL = 20
    await queue.addBulk(
      Array.from({ length: TOTAL }, (_, i) => ({ name: 'j', data: { n: i } })),
    )

    const done: number[] = []
    await new Promise<void>((resolve) => {
      worker.on('completed', (j) => {
        done.push((j.data as { n: number }).n)
        if (done.length === TOTAL) resolve()
      })
      worker.run().catch(() => {})
    })

    expect(done).toHaveLength(TOTAL)
  })

  it('retries on failure and eventually completes', async () => {
    let attempts = 0
    const retryWorker = new Worker<{ n: number }, void>(
      'basic',
      async () => {
        attempts++
        if (attempts < 3) throw new Error('transient')
      },
      { connection, autorun: false, drainDelay: 1 },
    )
    retryWorker.on('error', () => {})

    await queue.add('retry', { n: 0 }, { attempts: 3, backoff: { type: 'fixed', delay: 0 } })

    await new Promise<void>((resolve) => {
      retryWorker.on('completed', resolve)
      retryWorker.run().catch(() => {})
    })

    expect(attempts).toBe(3)
    await retryWorker.close()
  })

  it('marks job as failed after exhausting attempts', async () => {
    const failWorker = new Worker<{ n: number }, void>(
      'basic',
      async () => { throw new Error('always fails') },
      { connection, autorun: false, drainDelay: 1 },
    )
    failWorker.on('error', () => {})

    await queue.add('fail', { n: 0 }, { attempts: 2, backoff: { type: 'fixed', delay: 0 } })

    let failedReason: string | undefined
    await new Promise<void>((resolve) => {
      failWorker.on('failed', (j, err) => { failedReason = err.message; resolve() })
      failWorker.run().catch(() => {})
    })

    expect(failedReason).toBe('always fails')
    await failWorker.close()
  })
})

// ─── Concurrency ──────────────────────────────────────────────────────────────

describe('Worker concurrency', () => {
  afterEach(flush)

  it('never exceeds configured concurrency', async () => {
    const CONCURRENCY = 5
    const JOBS = 50
    const queue = new Queue<void, void>('conc', { connection })

    await queue.addBulk(Array.from({ length: JOBS }, () => ({ name: 'j', data: undefined })))

    let maxActive = 0
    let active = 0
    let done = 0

    const worker = new Worker<void, void>(
      'conc',
      async () => {
        active++
        if (active > maxActive) maxActive = active
        await new Promise((r) => setTimeout(r, 20))
        active--
        done++
      },
      { connection, autorun: false, drainDelay: 1, concurrency: CONCURRENCY },
    )
    worker.on('error', () => {})

    await new Promise<void>((resolve) => {
      worker.on('completed', () => { if (done === JOBS) resolve() })
      worker.run().catch(() => {})
    })

    expect(maxActive).toBeLessThanOrEqual(CONCURRENCY)
    expect(done).toBe(JOBS)

    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })
})

// ─── Groups ───────────────────────────────────────────────────────────────────

describe('Group scheduling', () => {
  afterEach(flush)

  it('maintains FIFO order within a group', async () => {
    const queue = new Queue<{ seq: number }, void>('group-fifo', { connection })
    const TOTAL = 30

    await queue.addBulk(
      Array.from({ length: TOTAL }, (_, i) => ({
        name: 'j',
        data: { seq: i },
        opts: { group: { id: 'g1' } },
      })),
    )

    const order: number[] = []
    const worker = new Worker<{ seq: number }, void>(
      'group-fifo',
      async (job) => { order.push(job.data.seq) },
      { connection, autorun: false, drainDelay: 1, concurrency: 1 },
    )
    worker.on('error', () => {})

    await new Promise<void>((resolve) => {
      worker.on('completed', () => { if (order.length === TOTAL) resolve() })
      worker.run().catch(() => {})
    })

    for (let i = 1; i < order.length; i++) {
      expect(order[i]).toBeGreaterThan(order[i - 1])
    }

    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('round-robins across groups fairly', async () => {
    const GROUPS = 4
    const JOBS_PER_GROUP = 10
    const queue = new Queue<{ g: number }, void>('group-rr', { connection })

    for (let g = 0; g < GROUPS; g++) {
      await queue.addBulk(
        Array.from({ length: JOBS_PER_GROUP }, () => ({
          name: 'j',
          data: { g },
          opts: { group: { id: `g${g}` } },
        })),
      )
    }

    const groupOrder: number[] = []
    let done = 0

    const worker = new Worker<{ g: number }, void>(
      'group-rr',
      async (job) => { groupOrder.push(job.data.g); done++ },
      { connection, autorun: false, drainDelay: 1, concurrency: 1 },
    )
    worker.on('error', () => {})

    await new Promise<void>((resolve) => {
      worker.on('completed', () => { if (done === GROUPS * JOBS_PER_GROUP) resolve() })
      worker.run().catch(() => {})
    })

    expect(done).toBe(GROUPS * JOBS_PER_GROUP)

    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('respects group concurrency (worker-level)', async () => {
    const GROUP_CONCURRENCY = 3
    const JOBS = 12
    const queue = new Queue<void, void>('group-conc', { connection })

    await queue.addBulk(
      Array.from({ length: JOBS }, () => ({
        name: 'j',
        data: undefined,
        opts: { group: { id: 'g1' } },
      })),
    )

    let maxActive = 0
    let active = 0
    let done = 0

    const worker = new Worker<void, void>(
      'group-conc',
      async () => {
        active++
        if (active > maxActive) maxActive = active
        await new Promise((r) => setTimeout(r, 30))
        active--
        done++
      },
      {
        connection,
        autorun: false,
        drainDelay: 1,
        concurrency: 10,
        group: { concurrency: GROUP_CONCURRENCY },
      },
    )
    worker.on('error', () => {})

    await new Promise<void>((resolve) => {
      worker.on('completed', () => { if (done === JOBS) resolve() })
      worker.run().catch(() => {})
    })

    expect(maxActive).toBeLessThanOrEqual(GROUP_CONCURRENCY)
    expect(done).toBe(JOBS)

    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })
})

// ─── BullMQ Pro group features ────────────────────────────────────────────────

describe('Group pauseGroup / resumeGroup', () => {
  afterEach(flush)

  it('pauses then resumes a group', async () => {
    const queue = new Queue<{ i: number }, void>('grp-pause', { connection })
    await queue.addBulk(
      Array.from({ length: 5 }, (_, i) => ({
        name: 't', data: { i }, opts: { group: { id: 'g' } },
      })),
    )
    await queue.pauseGroup('g')

    const processed: number[] = []
    const worker = new Worker<{ i: number }, void>(
      'grp-pause',
      async (job) => { processed.push(job.data.i) },
      { connection, autorun: false, drainDelay: 1 },
    )
    worker.on('error', () => {})
    worker.run().catch(() => {})

    await new Promise((r) => setTimeout(r, 400))
    expect(processed.length).toBe(0)

    await queue.resumeGroup('g')

    await new Promise<void>((resolve) => {
      worker.on('completed', () => { if (processed.length === 5) resolve() })
    })

    expect(processed.length).toBe(5)

    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })
})

describe('Group maxSize', () => {
  afterEach(flush)

  it('throws GroupMaxSizeExceededError when group is full', async () => {
    const queue = new Queue<{ i: number }, void>('grp-max', { connection })

    for (let i = 0; i < 3; i++) {
      await queue.add('t', { i }, { group: { id: 'g', maxSize: 3 } })
    }

    await expect(
      queue.add('t', { i: 3 }, { group: { id: 'g', maxSize: 3 } }),
    ).rejects.toBeInstanceOf(GroupMaxSizeExceededError)

    await queue.obliterate({ force: true })
    await queue.close()
  })
})

describe('Group intra-priority', () => {
  afterEach(flush)

  it('processes higher-priority jobs before lower-priority ones', async () => {
    const queue = new Queue<{ label: string }, void>('grp-pri', { connection })

    await queue.add('t', { label: 'low'  }, { group: { id: 'g', priority: 10 } })
    await queue.add('t', { label: 'high' }, { group: { id: 'g', priority: 1  } })
    await queue.add('t', { label: 'mid'  }, { group: { id: 'g', priority: 5  } })
    await queue.add('t', { label: 'fifo' }, { group: { id: 'g' } })

    const order: string[] = []
    const worker = new Worker<{ label: string }, void>(
      'grp-pri',
      async (job) => { order.push(job.data.label) },
      { connection, autorun: false, drainDelay: 1, concurrency: 1 },
    )
    worker.on('error', () => {})

    await new Promise<void>((resolve) => {
      worker.on('completed', () => { if (order.length === 4) resolve() })
      worker.run().catch(() => {})
    })

    expect(order[0]).toBe('high')
    expect(order[1]).toBe('mid')
    expect(order[2]).toBe('low')
    expect(order[3]).toBe('fifo')

    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })
})

describe('Local group concurrency (setGroupConcurrency)', () => {
  afterEach(flush)

  it('overrides global default per group', async () => {
    const queue = new Queue<{ g: string }, void>('local-conc', { connection })
    await queue.setGroupConcurrency('groupA', 3)

    const cfgA = await queue.getGroupConcurrency('groupA')
    const cfgB = await queue.getGroupConcurrency('groupB')

    expect(cfgA).toBe(3)
    expect(cfgB).toBeUndefined()

    await queue.close()
  })
})

describe('Manual group rate limit (Worker.RateLimitError)', () => {
  afterEach(flush)

  it('re-queues the job and delays processing', async () => {
    const queue = new Queue<{ i: number }, void>('rate-manual', { connection })
    await queue.addBulk(
      Array.from({ length: 3 }, (_, i) => ({
        name: 't', data: { i }, opts: { group: { id: 'g' } },
      })),
    )

    let rateLimited = false
    const completed: number[] = []

    const worker = new Worker<{ i: number }, void>(
      'rate-manual',
      async (job) => {
        if (job.data.i === 0 && !rateLimited) {
          rateLimited = true
          await worker.rateLimitGroup(job, 200)
          throw Worker.RateLimitError()
        }
        completed.push(job.data.i)
      },
      { connection, autorun: false, drainDelay: 1 },
    )
    worker.on('error', () => {})

    const t0 = Date.now()
    await new Promise<void>((resolve) => {
      worker.on('completed', () => { if (completed.length === 3) resolve() })
      worker.run().catch(() => {})
    })
    const elapsed = Date.now() - t0

    expect(completed).toHaveLength(3)
    expect(elapsed).toBeGreaterThanOrEqual(200)

    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })
})
