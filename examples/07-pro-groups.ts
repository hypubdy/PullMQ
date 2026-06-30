/**
 * Example 7: BullMQ Pro group features
 *
 * J. pauseGroup / resumeGroup
 * K. Global group rate limit (WorkerOptions.group.limit)
 * L. Manual rate limit (worker.rateLimitGroup + Worker.RateLimitError)
 * M. maxSize — GroupMaxSizeExceededError when group is full
 * N. Intra-group priority (group.priority)
 * O. Local group concurrency (queue.setGroupConcurrency / getGroupConcurrency)
 *
 * Run: npx tsx examples/07-pro-groups.ts
 */
import Redis from 'ioredis';
import { Queue, Worker } from '../src';
import { GroupMaxSizeExceededError, GroupRateLimitError } from '../src/errors';

const connection = { host: '127.0.0.1', port: 6379 };

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function assert(cond: boolean, msg: string) {
  if (!cond) throw new Error(`ASSERTION FAILED: ${msg}`);
}

// ─── J: pauseGroup / resumeGroup ─────────────────────────────────────────────

async function testGroupPauseResume() {
  console.log('══════════════════════════════════════════════');
  console.log(' J. pauseGroup / resumeGroup');
  console.log('══════════════════════════════════════════════');

  const queue = new Queue<{ i: number }, void>('pause-group', { connection });
  const GROUP = 'grp-a';

  // Add 5 jobs then immediately pause the group.
  await queue.addBulk(
    Array.from({ length: 5 }, (_, i) => ({
      name: 't', data: { i }, opts: { group: { id: GROUP } },
    })),
  );
  const paused = await queue.pauseGroup(GROUP);
  assert(paused, 'pauseGroup should return true on first pause');

  const alreadyPaused = await queue.pauseGroup(GROUP);
  assert(!alreadyPaused, 'pauseGroup should return false if already paused');

  const processed: number[] = [];
  const worker = new Worker<{ i: number }, void>(
    'pause-group',
    async (job) => { processed.push(job.data.i); },
    { connection, autorun: false, drainDelay: 1 },
  );
  worker.on('error', () => {});

  worker.run().catch(() => {});

  // Wait 400ms — group is paused so nothing should run.
  await sleep(400);
  assert(processed.length === 0, `Paused group should not process jobs (got ${processed.length})`);
  console.log('  Paused: 0 jobs ran in 400ms  ✓');

  // Resume the group.
  const resumed = await queue.resumeGroup(GROUP);
  assert(resumed, 'resumeGroup should return true');

  // Now wait for all 5 to complete.
  await new Promise<void>((resolve) => {
    worker.on('completed', () => { if (processed.length === 5) resolve(); });
  });

  assert(processed.length === 5, `After resume, all 5 jobs must run (got ${processed.length})`);
  console.log(`  Resumed: all 5 jobs completed  ✓`);

  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
}

// ─── K: Global group rate limit ───────────────────────────────────────────────

async function testGlobalGroupRateLimit() {
  console.log('\n══════════════════════════════════════════════');
  console.log(' K. Global group rate limit (WorkerOptions.group.limit)');
  console.log('══════════════════════════════════════════════');

  // limit: max 3 jobs per 500ms window
  const MAX = 3;
  const DURATION = 500;
  const TOTAL = 9; // 3 windows × 3 jobs

  const queue = new Queue<{ i: number }, void>('rate-global', { connection });
  const GROUP = 'rate-g';

  await queue.addBulk(
    Array.from({ length: TOTAL }, (_, i) => ({
      name: 't', data: { i }, opts: { group: { id: GROUP } },
    })),
  );

  const completedAt: number[] = [];
  const worker = new Worker<{ i: number }, void>(
    'rate-global',
    async () => { completedAt.push(Date.now()); },
    {
      connection,
      autorun: false,
      drainDelay: 1,
      concurrency: 10,
      group: { limit: { max: MAX, duration: DURATION } },
    },
  );
  worker.on('error', () => {});

  const t0 = Date.now();
  await new Promise<void>((resolve) => {
    worker.on('completed', () => { if (completedAt.length === TOTAL) resolve(); });
    worker.run().catch(() => {});
  });
  const elapsed = Date.now() - t0;

  // With 3 jobs/500ms and 9 total, we need at least 3 windows ≈ ≥1000ms.
  // Allow generous margin for timing.
  assert(completedAt.length === TOTAL, `All ${TOTAL} jobs should complete`);
  assert(elapsed >= DURATION * 2, `Rate limit should slow processing (elapsed=${elapsed}ms, expected≥${DURATION * 2}ms)`);

  console.log(`  Completed: ${completedAt.length}/${TOTAL}  ✓`);
  console.log(`  Elapsed  : ${elapsed}ms (rate=${MAX} jobs/${DURATION}ms)  ✓`);

  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
}

// ─── L: Manual rate limit via processor ──────────────────────────────────────

async function testManualRateLimit() {
  console.log('\n══════════════════════════════════════════════');
  console.log(' L. Manual rate limit (rateLimitGroup + RateLimitError)');
  console.log('══════════════════════════════════════════════');

  const queue = new Queue<{ i: number }, void>('rate-manual', { connection });
  const GROUP = 'manual-g';
  const TOTAL = 5;

  await queue.addBulk(
    Array.from({ length: TOTAL }, (_, i) => ({
      name: 't', data: { i }, opts: { group: { id: GROUP } },
    })),
  );

  let rateLimitHits = 0;
  const completed: number[] = [];

  const worker = new Worker<{ i: number }, void>(
    'rate-manual',
    async (job) => {
      // Rate limit after 2nd job — simulate a 429 from an API
      if (job.data.i === 2 && rateLimitHits === 0) {
        rateLimitHits++;
        await worker.rateLimitGroup(job, 300); // pause group for 300ms
        throw Worker.RateLimitError();
      }
      completed.push(job.data.i);
    },
    { connection, autorun: false, drainDelay: 1 },
  );
  worker.on('error', () => {});

  const t0 = Date.now();
  await new Promise<void>((resolve) => {
    worker.on('completed', () => { if (completed.length === TOTAL) resolve(); });
    worker.run().catch(() => {});
  });
  const elapsed = Date.now() - t0;

  // Job i=2 was rate-limited and re-queued — it should eventually complete.
  assert(completed.length === TOTAL, `All ${TOTAL} jobs should complete (got ${completed.length})`);
  assert(rateLimitHits === 1, `Rate limit should fire exactly once (got ${rateLimitHits})`);
  assert(elapsed >= 300, `Rate limit pause must be respected (elapsed=${elapsed}ms)`);

  console.log(`  Rate limit hits: ${rateLimitHits}  ✓`);
  console.log(`  Completed: ${completed.length}/${TOTAL}  ✓`);
  console.log(`  Elapsed  : ${elapsed}ms (≥300ms expected)  ✓`);

  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
}

// ─── M: Max group size ────────────────────────────────────────────────────────

async function testMaxGroupSize() {
  console.log('\n══════════════════════════════════════════════');
  console.log(' M. maxSize — GroupMaxSizeExceededError');
  console.log('══════════════════════════════════════════════');

  const queue = new Queue<{ i: number }, void>('max-size', { connection });
  const GROUP = 'bounded';
  const MAX_SIZE = 3;

  // Add 3 jobs (should succeed)
  for (let i = 0; i < MAX_SIZE; i++) {
    await queue.add('t', { i }, { group: { id: GROUP, maxSize: MAX_SIZE } });
  }
  console.log(`  Added ${MAX_SIZE} jobs (at limit)  ✓`);

  // 4th job should throw
  let threw = false;
  try {
    await queue.add('t', { i: MAX_SIZE }, { group: { id: GROUP, maxSize: MAX_SIZE } });
  } catch (err) {
    threw = err instanceof GroupMaxSizeExceededError;
  }
  assert(threw, `4th job should throw GroupMaxSizeExceededError`);
  console.log(`  4th job threw GroupMaxSizeExceededError  ✓`);

  // Also test with priority ZSET jobs
  const queue2 = new Queue<{ i: number }, void>('max-size-pri', { connection });
  const GROUP2 = 'bounded-pri';
  await queue2.add('t', { i: 0 }, { group: { id: GROUP2, priority: 1, maxSize: 2 } });
  await queue2.add('t', { i: 1 }, { group: { id: GROUP2, priority: 2, maxSize: 2 } });
  let threw2 = false;
  try {
    await queue2.add('t', { i: 2 }, { group: { id: GROUP2, priority: 3, maxSize: 2 } });
  } catch (err) {
    threw2 = err instanceof GroupMaxSizeExceededError;
  }
  assert(threw2, `3rd priority job should throw GroupMaxSizeExceededError`);
  console.log(`  Priority group maxSize also enforced  ✓`);

  await queue.obliterate({ force: true });
  await queue.close();
  await queue2.obliterate({ force: true });
  await queue2.close();
}

// ─── N: Intra-group priority ──────────────────────────────────────────────────

async function testIntraGroupPriority() {
  console.log('\n══════════════════════════════════════════════');
  console.log(' N. Intra-group priority (group.priority)');
  console.log('══════════════════════════════════════════════');

  const queue = new Queue<{ label: string; priority: number }, void>('intra-pri', { connection });
  const GROUP = 'pri-grp';

  // Add in this order: p=10 (low), p=1 (high), p=5 (mid), p=0 (FIFO)
  // Expected processing order by priority: p=1 → p=5 → p=10 → p=0 (FIFO last)
  await queue.add('t', { label: 'low',  priority: 10 }, { group: { id: GROUP, priority: 10 } });
  await queue.add('t', { label: 'high', priority: 1  }, { group: { id: GROUP, priority: 1  } });
  await queue.add('t', { label: 'mid',  priority: 5  }, { group: { id: GROUP, priority: 5  } });
  await queue.add('t', { label: 'fifo', priority: 0  }, { group: { id: GROUP } }); // no priority = FIFO

  // Verify getCountsPerPriorityForGroup
  const counts = await queue.getCountsPerPriorityForGroup(GROUP, [1, 5, 10, 0]);
  assert(counts['1'] === 1, `priority=1: expected 1 job (got ${counts['1']})`);
  assert(counts['5'] === 1, `priority=5: expected 1 job (got ${counts['5']})`);
  assert(counts['10'] === 1, `priority=10: expected 1 job (got ${counts['10']})`);
  assert(counts['0'] === 1, `priority=0 (FIFO): expected 1 job (got ${counts['0']})`);
  console.log(`  getCountsPerPriorityForGroup: ${JSON.stringify(counts)}  ✓`);

  const order: string[] = [];
  const worker = new Worker<{ label: string; priority: number }, void>(
    'intra-pri',
    async (job) => {
      order.push(job.data.label);
      await sleep(20); // ensure serial processing so order is deterministic
    },
    { connection, autorun: false, drainDelay: 1, concurrency: 1 },
  );
  worker.on('error', () => {});

  await new Promise<void>((resolve) => {
    worker.on('completed', () => { if (order.length === 4) resolve(); });
    worker.run().catch(() => {});
  });

  // Priority ZSET jobs first (lowest score = highest priority): p=1, p=5, p=10
  // Then FIFO LIST: fifo
  assert(order[0] === 'high', `First should be high-priority (got ${order[0]})`);
  assert(order[1] === 'mid',  `Second should be mid-priority (got ${order[1]})`);
  assert(order[2] === 'low',  `Third should be low-priority (got ${order[2]})`);
  assert(order[3] === 'fifo', `Fourth should be FIFO (got ${order[3]})`);

  console.log(`  Processing order: ${order.join(' → ')}  ✓`);

  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
}

// ─── O: Local group concurrency ───────────────────────────────────────────────

async function testLocalGroupConcurrency() {
  console.log('\n══════════════════════════════════════════════');
  console.log(' O. Local group concurrency (setGroupConcurrency / getGroupConcurrency)');
  console.log('══════════════════════════════════════════════');

  const queue = new Queue<{ g: string }, void>('local-conc', { connection });

  const GA = 'groupA'; // local override = 3
  const GB = 'groupB'; // falls back to worker default = 1

  // Set local concurrency for groupA only
  await queue.setGroupConcurrency(GA, 3);
  const cfgA = await queue.getGroupConcurrency(GA);
  const cfgB = await queue.getGroupConcurrency(GB);
  assert(cfgA === 3, `groupA local concurrency should be 3 (got ${cfgA})`);
  assert(cfgB === undefined, `groupB has no local config (got ${cfgB})`);
  console.log(`  setGroupConcurrency(groupA, 3) → getGroupConcurrency = ${cfgA}  ✓`);
  console.log(`  groupB (no override) → getGroupConcurrency = ${cfgB}  ✓`);

  const JOBS = 6;
  await queue.addBulk([
    ...Array.from({ length: JOBS }, () => ({ name: 't', data: { g: GA }, opts: { group: { id: GA } } })),
    ...Array.from({ length: JOBS }, () => ({ name: 't', data: { g: GB }, opts: { group: { id: GB } } })),
  ]);

  const maxConcurrentA = { val: 0 };
  const maxConcurrentB = { val: 0 };
  const activeA = { val: 0 };
  const activeB = { val: 0 };
  let totalDone = 0;

  const worker = new Worker<{ g: string }, void>(
    'local-conc',
    async (job) => {
      if (job.data.g === GA) {
        activeA.val++;
        if (activeA.val > maxConcurrentA.val) maxConcurrentA.val = activeA.val;
        await sleep(60);
        activeA.val--;
      } else {
        activeB.val++;
        if (activeB.val > maxConcurrentB.val) maxConcurrentB.val = activeB.val;
        await sleep(60);
        activeB.val--;
      }
      totalDone++;
    },
    {
      connection,
      autorun: false,
      drainDelay: 1,
      concurrency: 10,
      group: { concurrency: 1 }, // global default = 1
    },
  );
  worker.on('error', () => {});

  await new Promise<void>((resolve) => {
    worker.on('completed', () => { if (totalDone === JOBS * 2) resolve(); });
    worker.run().catch(() => {});
  });

  // groupA has local override=3 → up to 3 concurrent
  // groupB uses global default=1 → serial
  assert(maxConcurrentA.val > 1, `groupA (local=3) should run >1 concurrent (got ${maxConcurrentA.val})`);
  assert(maxConcurrentB.val <= 1, `groupB (default=1) should run ≤1 concurrent (got ${maxConcurrentB.val})`);
  assert(totalDone === JOBS * 2, `All ${JOBS * 2} jobs should complete`);

  console.log(`  groupA max concurrent: ${maxConcurrentA.val} (local override=3)  ✓`);
  console.log(`  groupB max concurrent: ${maxConcurrentB.val} (global default=1)  ✓`);
  console.log(`  Total completed: ${totalDone}/${JOBS * 2}  ✓`);

  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
}

// ─── Runner ───────────────────────────────────────────────────────────────────

async function main() {
  console.log('PullMQ — BullMQ Pro Group Features\n');

  const results: { name: string; passed: boolean; error?: string }[] = [];
  const run = async (name: string, fn: () => Promise<void>) => {
    // Flush Redis between tests
    const raw = new Redis(connection);
    await raw.flushall();
    await raw.quit();
    try {
      await fn();
      results.push({ name, passed: true });
    } catch (err) {
      console.error(`\n  ✗ ${(err as Error).message}`);
      results.push({ name, passed: false, error: (err as Error).message });
    }
  };

  await run('J. pauseGroup / resumeGroup',         testGroupPauseResume);
  await run('K. Global group rate limit',           testGlobalGroupRateLimit);
  await run('L. Manual rate limit (RateLimitError)', testManualRateLimit);
  await run('M. maxSize / GroupMaxSizeExceededError', testMaxGroupSize);
  await run('N. Intra-group priority',               testIntraGroupPriority);
  await run('O. Local group concurrency',            testLocalGroupConcurrency);

  console.log('\n══════════════════════════════════════════════');
  console.log(' SUMMARY');
  console.log('══════════════════════════════════════════════');
  let allPassed = true;
  for (const r of results) {
    const icon = r.passed ? '✓' : '✗';
    console.log(`  ${icon} ${r.name}${r.error ? `\n      → ${r.error}` : ''}`);
    if (!r.passed) allPassed = false;
  }
  console.log('══════════════════════════════════════════════');
  console.log(allPassed ? '  ALL TESTS PASSED ✓' : '  SOME TESTS FAILED ✗');
  console.log('══════════════════════════════════════════════\n');

  process.exit(allPassed ? 0 : 1);
}

main().catch((err) => { console.error(err); process.exit(1); });
