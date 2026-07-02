# @hypubdy/pullmq

[![CI](https://github.com/hypubdy/pullmq/actions/workflows/ci.yml/badge.svg)](https://github.com/hypubdy/pullmq/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/@hypubdy/pullmq?registry_uri=https://npm.pkg.github.com)](https://github.com/hypubdy/pullmq/packages)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Redis-based message queue with a **BullMQ-compatible API** and full **BullMQ Pro group scheduling** — open source, no license required.

## Features

- **Drop-in BullMQ replacement** — same `Queue`, `Worker`, `QueueEvents`, `Job` interface
- **BullMQ Pro groups** — round-robin, concurrency per group, pause/resume, rate limiting, intra-group priority, max group size
- **High throughput** — event-driven concurrency gate (no polling), ~3,000 jobs/s peak
- **Reliable** — stall detection & recovery, lock renewal, retry with backoff
- **TypeScript-first** — full type safety, dual ESM/CJS build

## Installation

Package được publish lên **GitHub Packages** (không phải npmjs.com), nên cần cấu hình registry trước.

### 1. Tạo file `.npmrc` trong thư mục dự án

```
@hypubdy:registry=https://npm.pkg.github.com
```

Hoặc chạy lệnh:

```bash
echo "@hypubdy:registry=https://npm.pkg.github.com" >> .npmrc
```

### 2. Đăng nhập GitHub Packages (chỉ cần làm một lần)

Tạo [GitHub Personal Access Token](https://github.com/settings/tokens/new) với quyền `read:packages`, sau đó:

```bash
npm login --registry=https://npm.pkg.github.com --scope=@hypubdy
# Username: <GitHub username>
# Password: <Personal Access Token>
```

Hoặc thêm token trực tiếp vào `~/.npmrc`:

```
//npm.pkg.github.com/:_authToken=YOUR_GITHUB_TOKEN
```

### 3. Cài đặt

```bash
npm install @hypubdy/pullmq ioredis
# or
pnpm add @hypubdy/pullmq ioredis
```

> Requires Node.js ≥ 18 and a running Redis ≥ 6.2 instance (atomic job pickup uses `LMOVE`/`BLMOVE`).

## Quick Start

```typescript
import { Queue, Worker } from '@hypubdy/pullmq'

const connection = { host: 'localhost', port: 6379 }

// Producer
const queue = new Queue('emails', { connection })
await queue.add('send', { to: 'user@example.com', subject: 'Hello' })

// Consumer
const worker = new Worker('emails', async (job) => {
  await sendEmail(job.data)
}, { connection, concurrency: 5 })

worker.on('completed', (job) => console.log(`Job ${job.id} done`))
worker.on('failed', (job, err) => console.error(`Job ${job.id} failed:`, err))
```

## Group Scheduling

Groups ensure jobs belonging to the same entity are processed fairly and in order — without one entity monopolising the worker pool.

```typescript
const queue = new Queue('notifications', { connection })

// Jobs for the same user are processed round-robin with other users
await queue.add('push', { userId: 'u1', msg: 'Hello' }, { group: { id: 'u1' } })
await queue.add('push', { userId: 'u2', msg: 'Hi'    }, { group: { id: 'u2' } })
```

### Group Concurrency

Control how many jobs from the same group run in parallel.

```typescript
// Global: all groups share the same concurrency cap
const worker = new Worker('q', processor, {
  connection,
  concurrency: 100,         // overall worker concurrency
  group: { concurrency: 3 }, // max 3 parallel jobs per group
})

// Local: override for a specific group
await queue.setGroupConcurrency('vip-user', 10)
const current = await queue.getGroupConcurrency('vip-user') // → 10
```

### Pause & Resume Groups

```typescript
await queue.pauseGroup('user-123')   // returns false if already paused
await queue.resumeGroup('user-123')  // returns false if not paused
```

Workers finish any in-progress job from the group before going idle.

### Rate Limiting

**Global rate limit** — applied automatically by the worker to every group:

```typescript
const worker = new Worker('q', processor, {
  connection,
  group: {
    limit: { max: 100, duration: 1000 }, // 100 jobs/second per group
  },
})
```

**Manual rate limit** — trigger from inside the processor (e.g. on a 429 response):

```typescript
const worker = new Worker('q', async (job) => {
  const res = await callExternalApi(job.data)

  if (res.status === 429) {
    await worker.rateLimitGroup(job, res.retryAfter * 1000)
    throw Worker.RateLimitError() // re-queues job, does not count as failure
  }
}, { connection })
```

Check the remaining rate-limit window:

```typescript
const ttlMs = await queue.getGroupRateLimitTtl('user-123') // 0 if not limited
```

### Max Group Size

Reject jobs when a group reaches a size limit:

```typescript
import { GroupMaxSizeExceededError } from '@hypubdy/pullmq'

try {
  await queue.add('task', data, { group: { id: 'user-123', maxSize: 1000 } })
} catch (err) {
  if (err instanceof GroupMaxSizeExceededError) {
    console.log('Group is full, drop or retry later')
  }
}
```

### Intra-Group Priority

Jobs within the same group can have different priorities. Lower number = higher priority (Unix nice convention). Priority jobs are always processed before FIFO jobs.

```typescript
await queue.add('task', data, { group: { id: 'g1', priority: 1  } }) // highest
await queue.add('task', data, { group: { id: 'g1', priority: 10 } }) // lower
await queue.add('task', data, { group: { id: 'g1' } })               // FIFO (processed last)

// Count jobs per priority level
const counts = await queue.getCountsPerPriorityForGroup('g1', [1, 10, 0])
// → { '1': 1, '10': 1, '0': 1 }
```

## API Reference

### `Queue`

| Method | Description |
|---|---|
| `add(name, data, opts?)` | Add a single job |
| `addBulk(jobs)` | Add multiple jobs |
| `getJob(id)` | Get a job by ID |
| `getJobCounts(...types)` | Get counts by state |
| `pause()` / `resume()` | Pause/resume the whole queue |
| `drain(delayed?)` | Remove all waiting jobs |
| `obliterate({ force })` | Delete queue and all jobs |
| `clean(grace, limit, type)` | Remove old jobs |
| `pauseGroup(id)` | Pause a specific group |
| `resumeGroup(id)` | Resume a specific group |
| `setGroupConcurrency(id, n)` | Set per-group concurrency |
| `getGroupConcurrency(id)` | Get per-group concurrency |
| `getGroupRateLimitTtl(id)` | Rate limit TTL (ms) for a group |
| `getCountsPerPriorityForGroup(id, priorities)` | Job counts by priority |

### `Worker`

| Option | Type | Default | Description |
|---|---|---|---|
| `concurrency` | `number` | `1` | Max parallel jobs |
| `group.concurrency` | `number` | `1` | Max parallel jobs per group |
| `group.limit` | `{ max, duration }` | — | Global group rate limit |
| `lockDuration` | `number` | `30000` | Job lock TTL (ms) |
| `stalledInterval` | `number` | `30000` | Stall check interval (ms) |
| `maxStalledCount` | `number` | `1` | Max stalls before permanent fail |
| `drainDelay` | `number` | `5` | BLMOVE timeout (s) |

| Method | Description |
|---|---|
| `run()` | Start processing |
| `pause(doNotWaitActive?)` | Pause the worker |
| `resume()` | Resume the worker |
| `close(force?)` | Graceful shutdown |
| `rateLimitGroup(job, ms)` | Rate limit a group from inside the processor |
| `Worker.RateLimitError()` | Error to throw to re-queue without failing |

### `JobsOptions`

```typescript
interface JobsOptions {
  delay?: number             // defer by ms
  priority?: number          // queue-level priority (lower = higher)
  attempts?: number          // max retry attempts
  backoff?: number | { type: 'fixed' | 'exponential'; delay: number }
  removeOnComplete?: boolean | number | { count?: number; age?: number }
  removeOnFail?: boolean | number | { count?: number; age?: number }
  group?: {
    id: string               // group identifier
    priority?: number        // intra-group priority (lower = higher)
    maxSize?: number         // max jobs in group before throwing
  }
}
```

### Errors

| Error | When thrown |
|---|---|
| `UnrecoverableError` | Thrown from processor — skips retry, marks job failed immediately |
| `GroupMaxSizeExceededError` | `queue.add()` when group has reached `maxSize` |
| `GroupRateLimitError` | Returned by `Worker.RateLimitError()`, re-queues the job |

## Performance

Measured on a local Redis instance with a no-op processor:

| Scenario | Throughput |
|---|---|
| concurrency=1 | ~800 jobs/s |
| concurrency=5 (peak) | ~3,200 jobs/s |
| Groups (concurrency/group=1) | ~1,000 jobs/s |
| Groups (concurrency/group=5) | ~2,100 jobs/s |

Run the benchmark yourself:

```bash
npm run bench
```

## Development

```bash
# Install
npm install

# Build (ESM + CJS)
npm run build

# Tests (requires Redis on localhost:6379)
npm test

# Watch mode
npm run dev
```

## License

MIT © hypubdy
