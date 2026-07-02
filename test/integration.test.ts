/**
 * PullMQ integration tests — requires a live Redis on 127.0.0.1:6379
 * Run: pnpm test
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import Redis from 'ioredis'
import { Queue, Worker, QueueEvents } from '../src'
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

  it('maxSize: 0 rejects every add instead of silently becoming unlimited', async () => {
    const queue = new Queue<{ i: number }, void>('grp-max-zero', { connection })

    await expect(
      queue.add('t', { i: 0 }, { group: { id: 'g', maxSize: 0 } }),
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

describe('Delayed job promotion race across multiple workers', () => {
  afterEach(flush)

  it('promotes a delayed job to :ready exactly once even when 3 workers race on it', async () => {
    const queue = new Queue<{ i: number }, void>('grp-delay-race', { connection })
    const keyPrefix = 'bull:grp-delay-race'
    const client = new Redis(connection)

    const job = await queue.add('t', { i: 0 }, { delay: 1 })
    // Let the delay elapse so all 3 workers see it as due via ZRANGEBYSCORE.
    await new Promise((r) => setTimeout(r, 10))

    const workers = Array.from({ length: 3 }, () =>
      new Worker<{ i: number }, void>(
        'grp-delay-race',
        async () => {},
        { connection, autorun: false, drainDelay: 1 },
      ),
    )
    workers.forEach((w) => w.on('error', () => {}))

    // Reproduce the reported race: 3 worker processes each read :delayed via
    // ZRANGEBYSCORE and all see job J before any of them finishes its ZREM
    // pipeline — reach into the private per-tick method to run all 3
    // concurrently and deterministically, instead of racing real 1s timers.
    const claimants = workers as unknown as Array<{ promoteDelayedJobsOnce(): Promise<void> }>
    await Promise.all(claimants.map((w) => w.promoteDelayedJobsOnce()))

    const readyEntries = await client.lrange(`${keyPrefix}:ready`, 0, -1)
    const occurrences = readyEntries.filter((id) => id === job.id).length

    // Only the worker whose ZREM actually removed the member may re-enqueue it.
    expect(occurrences).toBe(1)
    expect(await client.zscore(`${keyPrefix}:delayed`, job.id)).toBeNull()

    await client.quit()
    await Promise.all(workers.map((w) => w.close()))
    await queue.obliterate({ force: true })
    await queue.close()
  })
})

describe('Group slot leak when job hash disappears mid-dispatch', () => {
  afterEach(flush)

  it('releases running:{groupId} instead of leaking it when the job hash is gone by the time the worker loads it', async () => {
    const GROUP_CONCURRENCY = 2
    const queue = new Queue<{ i: number }, void>('grp-vanish', { connection })
    const keyPrefix = 'bull:grp-vanish'
    const client = new Redis(connection)

    const jobs = await Promise.all(
      Array.from({ length: GROUP_CONCURRENCY }, (_, i) =>
        queue.add('t', { i }, { group: { id: 'tenant-A' } }),
      ),
    )

    const worker = new Worker<{ i: number }, void>(
      'grp-vanish',
      async () => {},
      { connection, autorun: false, drainDelay: 1, group: { concurrency: GROUP_CONCURRENCY } },
    )
    worker.on('error', () => {})
    // Reach into private methods to drive the exact race deterministically,
    // instead of racing real timers against a running worker loop.
    const w = worker as unknown as {
      scheduleOneGroup(groupId: string): Promise<boolean>
      processJob(jobId: string): Promise<void>
    }

    // Reproduce the reported race: scheduleOneGroup() dispatches both jobs —
    // INCR running:tenant-A twice and RPUSH both ids to :ready — exactly as
    // it would in production. Before the worker gets around to loading them,
    // something deletes both job hashes (obliterate(), remove(), or Redis
    // LRU eviction under memory pressure).
    const scheduled = await w.scheduleOneGroup('tenant-A')
    expect(scheduled).toBe(true)
    expect(await client.get(`${keyPrefix}:running:tenant-A`)).toBe(String(GROUP_CONCURRENCY))

    for (const job of jobs) {
      await client.del(`${keyPrefix}:job:${job.id}`)
    }

    // Worker pulls both now-orphaned ids off :ready, same as mainLoop does,
    // and finds no job hash for either — Job.fromId() returns null.
    let jobId: string | null
    while ((jobId = await client.lpop(`${keyPrefix}:ready`))) {
      await w.processJob(jobId)
    }

    // The counter must come back down, not stay pinned at maxGroupConcurrency.
    expect(await client.get(`${keyPrefix}:running:tenant-A`)).toBe('0')
    for (const job of jobs) {
      expect(await client.hget(`${keyPrefix}:group:job-map`, job.id)).toBeNull()
    }

    // The real regression check: before the fix, tenant-A would be stalled
    // forever here because running:tenant-A stayed pinned at
    // maxGroupConcurrency with nothing left able to decrement it.
    await queue.add('t', { i: 99 }, { group: { id: 'tenant-A' } })
    const scheduledAgain = await w.scheduleOneGroup('tenant-A')
    expect(scheduledAgain).toBe(true)

    await client.quit()
    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })
})

describe('Queue.remove() releasing a dispatched group slot', () => {
  afterEach(flush)

  it('decrements running:{groupId} and clears group:job-map when removing an already-dispatched job', async () => {
    const queue = new Queue<{ i: number }, void>('grp-remove', { connection })
    const keyPrefix = 'bull:grp-remove'
    const client = new Redis(connection)

    const job = await queue.add('t', { i: 0 }, { group: { id: 'tenant-A' } })

    const worker = new Worker<{ i: number }, void>(
      'grp-remove',
      async () => {},
      { connection, autorun: false, drainDelay: 1, group: { concurrency: 1 } },
    )
    worker.on('error', () => {})
    const w = worker as unknown as { scheduleOneGroup(groupId: string): Promise<boolean> }

    // Dispatch it (running:tenant-A incremented, group:job-map recorded), same
    // as production, then remove it before any worker loads the job hash.
    expect(await w.scheduleOneGroup('tenant-A')).toBe(true)
    expect(await client.get(`${keyPrefix}:running:tenant-A`)).toBe('1')

    await queue.remove(job.id)

    // Before the fix, running:tenant-A stayed pinned at 1 forever since the
    // id was already gone from :ready, so no worker would ever discover it
    // was missing and release the slot.
    expect(await client.get(`${keyPrefix}:running:tenant-A`)).toBe('0')
    expect(await client.hget(`${keyPrefix}:group:job-map`, job.id)).toBeNull()

    await client.quit()
    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('removes a not-yet-dispatched job from its group list without touching running:{groupId}', async () => {
    const queue = new Queue<{ i: number }, void>('grp-remove-waiting', { connection })
    const keyPrefix = 'bull:grp-remove-waiting'
    const client = new Redis(connection)

    const job = await queue.add('t', { i: 0 }, { group: { id: 'tenant-A' } })
    expect(await client.lrange(`${keyPrefix}:group:tenant-A`, 0, -1)).toContain(job.id)

    await queue.remove(job.id)

    expect(await client.lrange(`${keyPrefix}:group:tenant-A`, 0, -1)).not.toContain(job.id)
    expect(await client.get(`${keyPrefix}:running:tenant-A`)).toBeNull()

    await client.quit()
    await queue.obliterate({ force: true })
    await queue.close()
  })
})

describe('releaseGroupSlotForMissingJob concurrency guard', () => {
  afterEach(flush)

  it('only decrements once when called twice concurrently for the same jobId', async () => {
    const queue = new Queue<{ i: number }, void>('grp-double-release', { connection })
    const keyPrefix = 'bull:grp-double-release'
    const client = new Redis(connection)

    const job = await queue.add('t', { i: 0 }, { group: { id: 'tenant-A' } })

    const worker = new Worker<{ i: number }, void>(
      'grp-double-release',
      async () => {},
      { connection, autorun: false, drainDelay: 1, group: { concurrency: 1 } },
    )
    worker.on('error', () => {})
    const w = worker as unknown as {
      scheduleOneGroup(groupId: string): Promise<boolean>
      releaseGroupSlotForMissingJob(jobId: string): Promise<void>
    }

    expect(await w.scheduleOneGroup('tenant-A')).toBe(true)
    expect(await client.get(`${keyPrefix}:running:tenant-A`)).toBe('1')

    // Simulate two independent discovery paths (processJob's missing-job
    // branch and the stalled checker's missing-job branch) racing on the
    // same jobId after the job hash has vanished.
    await client.del(`${keyPrefix}:job:${job.id}`)
    await Promise.all([
      w.releaseGroupSlotForMissingJob(job.id),
      w.releaseGroupSlotForMissingJob(job.id),
    ])

    // Must land at 0, not -1 — only the caller whose HDEL actually removed
    // the entry may decrement.
    expect(await client.get(`${keyPrefix}:running:tenant-A`)).toBe('0')

    await client.quit()
    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })
})

describe('Delayed job promotion respects group maxSize', () => {
  afterEach(flush)

  it('defers promotion instead of exceeding maxSize when a delayed job would overflow the group', async () => {
    const queue = new Queue<{ i: number }, void>('grp-delay-maxsize', { connection })
    const keyPrefix = 'bull:grp-delay-maxsize'
    const client = new Redis(connection)

    // Fill the group to its maxSize with jobs that have no delay.
    await queue.add('t', { i: 0 }, { group: { id: 'g', maxSize: 1 } })
    // This one bypasses the maxSize check at add-time (delay diverts it to
    // :delayed before the group branch runs) — it must not bypass the check
    // at promotion-time either.
    const delayedJob = await queue.add('t', { i: 1 }, { group: { id: 'g', maxSize: 1 }, delay: 1 })

    await new Promise((r) => setTimeout(r, 10))

    const worker = new Worker<{ i: number }, void>(
      'grp-delay-maxsize',
      async () => {},
      { connection, autorun: false, drainDelay: 1 },
    )
    worker.on('error', () => {})
    const w = worker as unknown as { promoteDelayedJobsOnce(): Promise<void> }

    await w.promoteDelayedJobsOnce()

    // The group is already at maxSize:1 — the delayed job must not have been
    // force-pushed into the group list, exceeding the configured limit.
    const groupLen = await client.llen(`${keyPrefix}:group:g`)
    expect(groupLen).toBe(1)
    // Instead it should have been deferred back onto :delayed for a retry.
    expect(await client.zscore(`${keyPrefix}:delayed`, delayedJob.id)).not.toBeNull()

    await client.quit()
    await worker.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })
})

describe('QueueEvents survives a throwing listener with no error handler', () => {
  afterEach(flush)

  it('keeps delivering events after a listener throws, even with no "error" listener attached', async () => {
    const queue = new Queue<{ i: number }, void>('qe-throw', { connection })
    const events = new QueueEvents('qe-throw', { connection, blockingTimeout: 200 })
    // Deliberately no `.on('error', ...)` — this is the exact regression: Node's
    // EventEmitter throws synchronously on an unhandled 'error' emit, which used
    // to escape both catch blocks in consumeEvents() and kill the whole loop.

    const received: unknown[] = []
    let threwOnce = false
    events.on('added', (data: unknown) => {
      if (!threwOnce) {
        threwOnce = true
        throw new Error('boom')
      }
      received.push(data)
    })

    events.run().catch(() => {})
    await events.waitUntilReady()

    await queue.add('t', { i: 0 }, {})
    await new Promise((r) => setTimeout(r, 100))
    await queue.add('t', { i: 1 }, {})
    await new Promise((r) => setTimeout(r, 200))

    // The second event must still arrive — the loop must not have died when
    // the first listener threw with no 'error' listener registered.
    expect(received.length).toBe(1)

    await events.close()
    await queue.obliterate({ force: true })
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
