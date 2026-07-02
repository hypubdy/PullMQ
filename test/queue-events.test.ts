/**
 * QueueEvents delivery tests — requires a live Redis on 127.0.0.1:6379
 * Run: pnpm test
 */
import { describe, it, expect, beforeEach } from 'vitest'
import Redis from 'ioredis'
import { Queue, Worker, QueueEvents } from '../src'

const connection = { host: '127.0.0.1', port: 6379 }

async function flush() {
  const r = new Redis(connection)
  await r.flushall()
  await r.quit()
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms))
}

async function until(cond: () => boolean, ms = 5000) {
  const t0 = Date.now()
  while (!cond()) {
    if (Date.now() - t0 > ms) throw new Error('condition not met in time')
    await sleep(25)
  }
}

type EventData = Record<string, unknown>

describe('QueueEvents delivery', () => {
  beforeEach(flush)

  it('delivers added, completed and failed events end-to-end', async () => {
    const queue = new Queue<{ fail: boolean }, number>('qe-e2e', { connection })
    // lastEventId '0' reads the stream from the beginning, so delivery does not
    // depend on the consumer's XREAD being in flight before events are published.
    const events = new QueueEvents('qe-e2e', { connection, blockingTimeout: 200, lastEventId: '0' })
    events.on('error', () => {})

    const added: EventData[] = []
    const completed: EventData[] = []
    const failed: EventData[] = []
    events.on('added', (d: EventData) => added.push(d))
    events.on('completed', (d: EventData) => completed.push(d))
    events.on('failed', (d: EventData) => failed.push(d))
    events.run().catch(() => {})

    const okJob = await queue.add('ok', { fail: false })
    const badJob = await queue.add('bad', { fail: true })

    const worker = new Worker<{ fail: boolean }, number>(
      'qe-e2e',
      async (job) => {
        if (job.data.fail) throw new Error('boom')
        return 7
      },
      { connection, autorun: false, drainDelay: 1 },
    )
    worker.on('error', () => {})
    worker.run().catch(() => {})

    await until(() => added.length === 2 && completed.length === 1 && failed.length === 1)

    expect(added.map((d) => d.jobId).sort()).toEqual([okJob.id, badJob.id].sort())
    expect(completed[0].jobId).toBe(okJob.id)
    expect(completed[0].returnvalue).toBe('7') // stream payload carries the JSON-encoded return value
    expect(failed[0].jobId).toBe(badJob.id)
    expect(failed[0].failedReason).toBe('boom')

    await worker.close()
    await events.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it("lastEventId: '0' replays events published before the consumer started", async () => {
    const queue = new Queue<{ i: number }, void>('qe-replay', { connection })

    const j1 = await queue.add('t', { i: 0 })
    const j2 = await queue.add('t', { i: 1 })

    const events = new QueueEvents('qe-replay', { connection, blockingTimeout: 200, lastEventId: '0' })
    events.on('error', () => {})
    const added: EventData[] = []
    events.on('added', (d: EventData) => added.push(d))
    events.run().catch(() => {})

    await until(() => added.length === 2)
    expect(added.map((d) => d.jobId)).toEqual([j1.id, j2.id])

    await events.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it("delivers 'progress' events published via updateJobProgress", async () => {
    const queue = new Queue<{ i: number }, void>('qe-progress', { connection })
    const events = new QueueEvents('qe-progress', { connection, blockingTimeout: 200, lastEventId: '0' })
    events.on('error', () => {})

    const progress: EventData[] = []
    events.on('progress', (d: EventData) => progress.push(d))
    events.run().catch(() => {})

    const job = await queue.add('t', { i: 0 })
    await queue.updateJobProgress(job.id, { pct: 50 })

    await until(() => progress.length === 1)
    expect(progress[0].jobId).toBe(job.id)
    expect(progress[0].data).toEqual({ pct: 50 })

    await events.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('a throwing "error" listener does not kill the read loop', async () => {
    const queue = new Queue<{ i: number }, void>('qe-err-throw', { connection })
    const events = new QueueEvents('qe-err-throw', { connection, blockingTimeout: 200, lastEventId: '0' })

    const received: EventData[] = []
    let threwOnce = false
    events.on('added', (d: EventData) => {
      if (!threwOnce) {
        threwOnce = true
        throw new Error('listener boom') // routed to the 'error' listener...
      }
      received.push(d)
    })
    // ...which itself throws — before the fix this escaped the catch and
    // killed the whole consumeEvents() loop.
    events.on('error', () => { throw new Error('error handler boom') })
    events.run().catch(() => {})

    await queue.add('t', { i: 0 })
    await sleep(200)
    await queue.add('t', { i: 1 })

    await until(() => received.length === 1)
    expect(received).toHaveLength(1)

    await events.close()
    await queue.obliterate({ force: true })
    await queue.close()
  })

  it('close() stops event delivery', async () => {
    const queue = new Queue<{ i: number }, void>('qe-close', { connection })
    const events = new QueueEvents('qe-close', { connection, blockingTimeout: 200, lastEventId: '0' })
    events.on('error', () => {})

    const added: EventData[] = []
    events.on('added', (d: EventData) => added.push(d))
    events.run().catch(() => {})

    await queue.add('t', { i: 0 })
    await until(() => added.length === 1)

    await events.close()

    await queue.add('t', { i: 1 })
    await sleep(500)
    expect(added).toHaveLength(1)

    await queue.obliterate({ force: true })
    await queue.close()
  })
})
