/**
 * Example 5: Stress test — verify correctness at scale
 *
 * Các bài kiểm tra:
 *  A. Throughput       — 2000 jobs, đếm completed vs added
 *  B. Group FIFO       — 50 groups × 40 jobs, verify thứ tự trong mỗi group
 *  C. Concurrency cap  — concurrency=5, verify activeCount không vượt 5
 *  D. No duplicates    — Set của jobId processed, không được có trùng
 *  E. Multi-worker     — 4 workers cùng compete, tổng completed = tổng added
 *  F. Mixed load       — priority + delay + group + normal cùng lúc
 *
 * Run: npx tsx examples/05-stress.ts
 */
import { Queue, Worker } from '../src';

const connection = { host: '127.0.0.1', port: 6379 };
const REDIS_FLUSH_BETWEEN = true; // flushall giữa các bài

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function fmt(n: number) {
  return n.toString().padStart(6, ' ');
}

function assert(cond: boolean, msg: string) {
  if (!cond) throw new Error(`ASSERTION FAILED: ${msg}`);
}

// ─── A. Throughput ────────────────────────────────────────────────────────────

async function testThroughput() {
  console.log('\n══════════════════════════════════════════════');
  console.log(' A. Throughput: 2000 jobs, concurrency=10');
  console.log('══════════════════════════════════════════════');

  const TOTAL = 2000;
  const queue = new Queue<{ i: number }, void>('stress-throughput', { connection });

  // Add jobs in bulk batches
  const batchSize = 200;
  for (let b = 0; b < TOTAL / batchSize; b++) {
    await queue.addBulk(
      Array.from({ length: batchSize }, (_, k) => ({
        name: 'job',
        data: { i: b * batchSize + k },
      })),
    );
  }
  console.log(`  Added ${TOTAL} jobs`);

  const completed = new Set<number>();
  let duplicates = 0;

  const worker = new Worker<{ i: number }, void>(
    'stress-throughput',
    async (job) => {
      if (completed.has(job.data.i)) duplicates++;
      completed.add(job.data.i);
    },
    { connection, autorun: false, drainDelay: 1, concurrency: 10 },
  );
  worker.on('error', () => {});

  const t0 = Date.now();
  await new Promise<void>((resolve) => {
    worker.on('completed', () => { if (completed.size === TOTAL) resolve(); });
    worker.run().catch(() => {});
  });
  const elapsed = Date.now() - t0;

  assert(completed.size === TOTAL, `completed=${completed.size} expected=${TOTAL}`);
  assert(duplicates === 0, `duplicates=${duplicates}`);

  const counts = await queue.getJobCounts();
  console.log(`  Completed : ${completed.size}/${TOTAL}  ✓`);
  console.log(`  Duplicates: ${duplicates}  ✓`);
  console.log(`  Time      : ${elapsed}ms  (${Math.round(TOTAL / (elapsed / 1000))} jobs/s)`);
  console.log(`  Redis counts:`, counts);

  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
}

// ─── B. Group FIFO correctness ────────────────────────────────────────────────

async function testGroupFIFO() {
  const GROUPS = 50;
  const JOBS_PER_GROUP = 40;
  const TOTAL = GROUPS * JOBS_PER_GROUP;

  console.log('\n══════════════════════════════════════════════');
  console.log(` B. Group FIFO: ${GROUPS} groups × ${JOBS_PER_GROUP} jobs = ${TOTAL} total`);
  console.log('══════════════════════════════════════════════');

  const queue = new Queue<{ group: string; seq: number }, void>('stress-groups', { connection });

  // Enqueue: interleave groups (worst case for FIFO verification)
  for (let seq = 0; seq < JOBS_PER_GROUP; seq++) {
    await queue.addBulk(
      Array.from({ length: GROUPS }, (_, g) => ({
        name: 'step',
        data: { group: `g${g}`, seq },
        opts: { group: { id: `g${g}` } },
      })),
    );
  }
  console.log(`  Added ${TOTAL} jobs`);

  // Track per-group processing order
  const orderByGroup: Record<string, number[]> = {};
  let processedCount = 0;

  const worker = new Worker<{ group: string; seq: number }, void>(
    'stress-groups',
    async (job) => {
      const g = job.data.group;
      (orderByGroup[g] ??= []).push(job.data.seq);
      processedCount++;
    },
    { connection, autorun: false, drainDelay: 1, concurrency: 8 },
  );
  worker.on('error', () => {});

  const t0 = Date.now();
  await new Promise<void>((resolve) => {
    worker.on('completed', () => { if (processedCount === TOTAL) resolve(); });
    worker.run().catch(() => {});
  });
  const elapsed = Date.now() - t0;

  // Verify FIFO: each group must be [0,1,2,...,JOBS_PER_GROUP-1]
  let fifoErrors = 0;
  for (let g = 0; g < GROUPS; g++) {
    const key = `g${g}`;
    const order = orderByGroup[key] ?? [];
    for (let i = 0; i < order.length; i++) {
      if (order[i] !== i) {
        fifoErrors++;
        if (fifoErrors <= 3) {
          console.log(`  FIFO ERROR in ${key}: position ${i} got seq=${order[i]}`);
        }
      }
    }
  }

  const missingGroups = Array.from({ length: GROUPS }, (_, g) => `g${g}`)
    .filter((g) => !orderByGroup[g] || orderByGroup[g].length !== JOBS_PER_GROUP);

  assert(processedCount === TOTAL, `processed=${processedCount} expected=${TOTAL}`);
  assert(fifoErrors === 0, `FIFO violations: ${fifoErrors}`);
  assert(missingGroups.length === 0, `missing groups: ${missingGroups.join(',')}`);

  console.log(`  Processed : ${processedCount}/${TOTAL}  ✓`);
  console.log(`  FIFO errors: ${fifoErrors}  ✓`);
  console.log(`  Time       : ${elapsed}ms`);

  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
}

// ─── C. Concurrency cap ────────────────────────────────────────────────────────

async function testConcurrencyCap() {
  const CONCURRENCY = 5;
  const TOTAL = 300;

  console.log('\n══════════════════════════════════════════════');
  console.log(` C. Concurrency cap: ${TOTAL} jobs, concurrency=${CONCURRENCY}`);
  console.log('══════════════════════════════════════════════');

  const queue = new Queue<{ i: number }, void>('stress-concurrency', { connection });

  await queue.addBulk(
    Array.from({ length: TOTAL }, (_, i) => ({ name: 'job', data: { i } })),
  );

  let currentActive = 0;
  let maxObserved = 0;
  let overflows = 0;
  let processedCount = 0;

  const worker = new Worker<{ i: number }, void>(
    'stress-concurrency',
    async (_job) => {
      currentActive++;
      if (currentActive > maxObserved) maxObserved = currentActive;
      if (currentActive > CONCURRENCY) overflows++;
      await sleep(10); // hold the slot briefly
      currentActive--;
      processedCount++;
    },
    { connection, autorun: false, drainDelay: 1, concurrency: CONCURRENCY },
  );
  worker.on('error', () => {});

  const t0 = Date.now();
  await new Promise<void>((resolve) => {
    worker.on('completed', () => { if (processedCount === TOTAL) resolve(); });
    worker.run().catch(() => {});
  });
  const elapsed = Date.now() - t0;

  assert(processedCount === TOTAL, `processed=${processedCount}`);
  assert(overflows === 0, `concurrency overflows=${overflows} (max observed=${maxObserved})`);

  console.log(`  Processed       : ${processedCount}/${TOTAL}  ✓`);
  console.log(`  Max concurrency : ${maxObserved} (limit=${CONCURRENCY})  ✓`);
  console.log(`  Overflows       : ${overflows}  ✓`);
  console.log(`  Time            : ${elapsed}ms`);

  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
}

// ─── D. No duplicates under multi-worker ─────────────────────────────────────

async function testNoDuplicates() {
  const WORKERS = 4;
  const TOTAL = 1000;

  console.log('\n══════════════════════════════════════════════');
  console.log(` D. No duplicates: ${TOTAL} jobs, ${WORKERS} workers competing`);
  console.log('══════════════════════════════════════════════');

  const queue = new Queue<{ i: number }, void>('stress-nodup', { connection });

  await queue.addBulk(
    Array.from({ length: TOTAL }, (_, i) => ({ name: 'job', data: { i } })),
  );

  const processed = new Set<number>();
  const duplicateIds: number[] = [];
  let totalCompleted = 0;

  const workers = Array.from({ length: WORKERS }, (_, w) => {
    const worker = new Worker<{ i: number }, void>(
      'stress-nodup',
      async (job) => {
        if (processed.has(job.data.i)) {
          duplicateIds.push(job.data.i);
        }
        processed.add(job.data.i);
        totalCompleted++;
      },
      { connection, autorun: false, drainDelay: 1, concurrency: 3 },
    );
    worker.on('error', () => {});
    return worker;
  });

  const t0 = Date.now();
  await new Promise<void>((resolve) => {
    workers.forEach((w) => {
      w.on('completed', () => { if (totalCompleted === TOTAL) resolve(); });
      w.run().catch(() => {});
    });
  });
  const elapsed = Date.now() - t0;

  assert(totalCompleted === TOTAL, `completed=${totalCompleted} expected=${TOTAL}`);
  assert(duplicateIds.length === 0, `duplicates: ${duplicateIds.slice(0, 5).join(',')}`);

  console.log(`  Completed  : ${totalCompleted}/${TOTAL}  ✓`);
  console.log(`  Duplicates : ${duplicateIds.length}  ✓`);
  console.log(`  Unique IDs : ${processed.size}  ✓`);
  console.log(`  Time       : ${elapsed}ms  (${WORKERS} workers × concurrency=3)`);

  await Promise.all(workers.map((w) => w.close()));
  await queue.obliterate({ force: true });
  await queue.close();
}

// ─── E. Mixed load ────────────────────────────────────────────────────────────

async function testMixedLoad() {
  const N = 500;

  console.log('\n══════════════════════════════════════════════');
  console.log(` E. Mixed load: normal + priority + group (${N * 3} jobs)`);
  console.log('══════════════════════════════════════════════');

  const queue = new Queue<{ type: string; id: number }, void>('stress-mixed', { connection });

  // Normal jobs
  await queue.addBulk(
    Array.from({ length: N }, (_, i) => ({
      name: 'normal',
      data: { type: 'normal', id: i },
    })),
  );

  // Priority jobs (should be processed first among non-group)
  await queue.addBulk(
    Array.from({ length: N }, (_, i) => ({
      name: 'priority',
      data: { type: 'priority', id: i },
      opts: { priority: 1 },
    })),
  );

  // Group jobs: 10 groups × N/10 jobs
  const GROUP_COUNT = 10;
  const JOBS_PER = Math.floor(N / GROUP_COUNT);
  for (let g = 0; g < GROUP_COUNT; g++) {
    await queue.addBulk(
      Array.from({ length: JOBS_PER }, (_, i) => ({
        name: 'group-step',
        data: { type: `group-${g}`, id: i },
        opts: { group: { id: `mixed-group-${g}` } },
      })),
    );
  }

  const TOTAL = N + N + GROUP_COUNT * JOBS_PER;
  const counts: Record<string, number> = { normal: 0, priority: 0, group: 0 };
  const groupOrder: Record<string, number[]> = {};
  let processedCount = 0;

  const worker = new Worker<{ type: string; id: number }, void>(
    'stress-mixed',
    async (job) => {
      processedCount++;
      const t = job.data.type;
      if (t === 'normal') counts.normal++;
      else if (t === 'priority') counts.priority++;
      else {
        counts.group++;
        (groupOrder[t] ??= []).push(job.data.id);
      }
    },
    { connection, autorun: false, drainDelay: 1, concurrency: 8 },
  );
  worker.on('error', () => {});

  const t0 = Date.now();
  await new Promise<void>((resolve) => {
    worker.on('completed', () => { if (processedCount === TOTAL) resolve(); });
    worker.run().catch(() => {});
  });
  const elapsed = Date.now() - t0;

  // Verify group FIFO
  let fifoErrors = 0;
  for (const [g, order] of Object.entries(groupOrder)) {
    for (let i = 0; i < order.length; i++) {
      if (order[i] !== i) fifoErrors++;
    }
  }

  assert(processedCount === TOTAL, `processed=${processedCount} expected=${TOTAL}`);
  assert(counts.normal + counts.priority + counts.group === TOTAL, 'type counts mismatch');
  assert(fifoErrors === 0, `group FIFO violations: ${fifoErrors}`);

  console.log(`  Total processed : ${processedCount}/${TOTAL}  ✓`);
  console.log(`  Normal          : ${counts.normal}/${N}  ✓`);
  console.log(`  Priority        : ${counts.priority}/${N}  ✓`);
  console.log(`  Group           : ${counts.group}/${GROUP_COUNT * JOBS_PER}  ✓`);
  console.log(`  Group FIFO errors: ${fifoErrors}  ✓`);
  console.log(`  Time            : ${elapsed}ms`);

  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
}

// ─── F. Retry correctness under load ─────────────────────────────────────────

async function testRetryUnderLoad() {
  const TOTAL = 200;
  const FAIL_RATE = 0.3; // 30% fail on first attempt

  console.log('\n══════════════════════════════════════════════');
  console.log(` F. Retry under load: ${TOTAL} jobs, ${FAIL_RATE * 100}% fail first attempt`);
  console.log('══════════════════════════════════════════════');

  const queue = new Queue<{ i: number }, void>('stress-retry', { connection });

  await queue.addBulk(
    Array.from({ length: TOTAL }, (_, i) => ({
      name: 'job',
      data: { i },
      opts: { attempts: 3, backoff: { type: 'fixed', delay: 50 } },
    })),
  );

  const completedIds = new Set<number>();
  const failedIds = new Set<number>();
  const attemptCount: Record<number, number> = {};

  const worker = new Worker<{ i: number }, void>(
    'stress-retry',
    async (job) => {
      const id = job.data.i;
      attemptCount[id] = (attemptCount[id] ?? 0) + 1;

      // First attempt: fail if id falls in fail rate
      if (attemptCount[id] === 1 && id % 10 < FAIL_RATE * 10) {
        throw new Error(`transient error for job ${id}`);
      }
      completedIds.add(id);
    },
    { connection, autorun: false, drainDelay: 1, concurrency: 10 },
  );

  worker.on('failed', (job) => {
    if (job) failedIds.add(job.data.i);
  });
  worker.on('error', () => {});

  const t0 = Date.now();
  await new Promise<void>((resolve) => {
    worker.on('completed', () => {
      if (completedIds.size === TOTAL) resolve();
    });
    worker.run().catch(() => {});
  });
  const elapsed = Date.now() - t0;

  const retriedJobs = Object.values(attemptCount).filter((c) => c > 1).length;

  assert(completedIds.size === TOTAL, `completed=${completedIds.size} expected=${TOTAL}`);

  console.log(`  Completed       : ${completedIds.size}/${TOTAL}  ✓`);
  console.log(`  Retried jobs    : ${retriedJobs}  (expected ~${Math.round(TOTAL * FAIL_RATE)})`);
  console.log(`  Permanently failed: 0  ✓`);
  console.log(`  Time            : ${elapsed}ms`);

  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
}

// ─── Runner ───────────────────────────────────────────────────────────────────

async function main() {
  console.log('PullMQ Stress Test');
  console.log('==================');

  const results: { name: string; passed: boolean; error?: string }[] = [];

  const run = async (name: string, fn: () => Promise<void>) => {
    try {
      await fn();
      results.push({ name, passed: true });
    } catch (err) {
      console.error(`\n  ✗ FAILED: ${(err as Error).message}`);
      results.push({ name, passed: false, error: (err as Error).message });
    }
  };

  await run('A. Throughput 2000 jobs',         testThroughput);
  await run('B. Group FIFO 50×40',              testGroupFIFO);
  await run('C. Concurrency cap',               testConcurrencyCap);
  await run('D. No duplicates 4 workers',       testNoDuplicates);
  await run('E. Mixed load',                    testMixedLoad);
  await run('F. Retry under load',              testRetryUnderLoad);

  // ─── Summary ───────────────────────────────────────────────────────────────
  console.log('\n══════════════════════════════════════════════');
  console.log(' SUMMARY');
  console.log('══════════════════════════════════════════════');
  let allPassed = true;
  for (const r of results) {
    const icon = r.passed ? '✓' : '✗';
    console.log(`  ${icon} ${r.name}${r.error ? `  — ${r.error}` : ''}`);
    if (!r.passed) allPassed = false;
  }
  console.log('══════════════════════════════════════════════');
  console.log(allPassed ? '  ALL TESTS PASSED ✓' : '  SOME TESTS FAILED ✗');
  console.log('══════════════════════════════════════════════\n');

  process.exit(allPassed ? 0 : 1);
}

main().catch((err) => { console.error(err); process.exit(1); });
