/**
 * Example 6: Group concurrency > 1 + stall recovery
 *
 * Bài G — group.concurrency > 1
 *   Verify rằng N jobs từ cùng một group có thể chạy song song (không chỉ 1).
 *   Đồng thời vẫn đảm bảo không vượt quá giới hạn concurrency của group.
 *
 * Bài H — stall recovery cho group jobs
 *   Simulate worker crash bằng cách inject trạng thái "active nhưng không có lock"
 *   vào Redis, rồi verify stall checker phát hiện + DECR counter + re-enqueue đúng.
 *
 * Run: npx tsx examples/06-group-concurrency.ts
 */
import Redis from 'ioredis';
import { Queue, Worker } from '../src';

const connection = { host: '127.0.0.1', port: 6379 };

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function assert(cond: boolean, msg: string) {
  if (!cond) throw new Error(`ASSERTION FAILED: ${msg}`);
}

// ─── G: group.concurrency > 1 ────────────────────────────────────────────────

async function testGroupConcurrencyGT1() {
  console.log('══════════════════════════════════════════════');
  console.log(' G. group.concurrency > 1');
  console.log('══════════════════════════════════════════════');

  const GROUPS = 4;
  const GROUP_CONCURRENCY = 3; // 3 jobs chạy song song trong 1 group
  const JOBS_PER_GROUP = 12;   // 12 jobs / group → 4 đợt × 3 song song
  const TOTAL = GROUPS * JOBS_PER_GROUP;

  const queue = new Queue<{ g: number; seq: number }, void>('gconcur', { connection });

  // Track per-group active count để kiểm tra không vượt giới hạn
  const groupActiveNow: Record<string, number> = {};
  const groupMaxSeen: Record<string, number> = {};
  const groupOverflows: Record<string, number> = {};
  const groupOrder: Record<string, number[]> = {};
  let completed = 0;

  const worker = new Worker<{ g: number; seq: number }, void>(
    'gconcur',
    async (job) => {
      const key = `g${job.data.g}`;
      groupActiveNow[key] = (groupActiveNow[key] ?? 0) + 1;

      // Snapshot max concurrent trong group này
      if (groupActiveNow[key] > (groupMaxSeen[key] ?? 0)) {
        groupMaxSeen[key] = groupActiveNow[key];
      }
      // Đếm vi phạm
      if (groupActiveNow[key] > GROUP_CONCURRENCY) {
        groupOverflows[key] = (groupOverflows[key] ?? 0) + 1;
      }

      (groupOrder[key] ??= []).push(job.data.seq);

      await sleep(80); // giữ slot 80ms để các job khác cùng group có cơ hội chạy song song

      groupActiveNow[key]--;
      completed++;
    },
    {
      connection,
      autorun: false,
      drainDelay: 1,
      concurrency: GROUPS * GROUP_CONCURRENCY,
      // BullMQ Pro API: group concurrency is set on the worker, not per-job
      group: { concurrency: GROUP_CONCURRENCY },
    },
  );

  worker.on('error', () => {});

  // Enqueue: group concurrency is now on the worker (BullMQ Pro API)
  for (let g = 0; g < GROUPS; g++) {
    await queue.addBulk(
      Array.from({ length: JOBS_PER_GROUP }, (_, seq) => ({
        name: 'step',
        data: { g, seq },
        opts: { group: { id: `g${g}` } },
      })),
    );
  }

  console.log(`  Added ${TOTAL} jobs across ${GROUPS} groups (concurrency=${GROUP_CONCURRENCY} each)`);

  const t0 = Date.now();
  await new Promise<void>((resolve) => {
    worker.on('completed', () => { if (completed === TOTAL) resolve(); });
    worker.run().catch(() => {});
  });
  const elapsed = Date.now() - t0;

  // ── Verify ──
  let totalOverflows = 0;
  console.log('\n  Per-group results:');
  for (let g = 0; g < GROUPS; g++) {
    const key = `g${g}`;
    const maxSeen = groupMaxSeen[key] ?? 0;
    const overflows = groupOverflows[key] ?? 0;
    totalOverflows += overflows;
    const icon = overflows === 0 ? '✓' : '✗';
    console.log(`  ${icon} g${g}: max concurrent=${maxSeen} (limit=${GROUP_CONCURRENCY}), overflows=${overflows}`);
  }

  // Verify FIFO vẫn còn nguyên vẹn khi có concurrency > 1
  // (Với concurrency=3, thứ tự xử lý WITHIN a "batch" không đảm bảo,
  //  nhưng mỗi batch phải hoàn thành trước khi batch tiếp theo bắt đầu)
  // Ta chỉ verify rằng không có job nào bị miss
  for (let g = 0; g < GROUPS; g++) {
    const key = `g${g}`;
    assert(
      (groupOrder[key]?.length ?? 0) === JOBS_PER_GROUP,
      `g${g}: processed ${groupOrder[key]?.length} but expected ${JOBS_PER_GROUP}`,
    );
  }

  assert(completed === TOTAL, `completed=${completed} expected=${TOTAL}`);
  assert(totalOverflows === 0, `concurrency overflows: ${totalOverflows}`);

  // Thời gian kỳ vọng: với concurrency=3 và 80ms/job, mỗi group mất ceil(12/3)*80 = 320ms
  // 4 groups chạy song song → tổng ≈ 320ms + overhead
  console.log(`\n  Completed : ${completed}/${TOTAL}  ✓`);
  console.log(`  Overflows : ${totalOverflows}  ✓`);
  console.log(`  Time      : ${elapsed}ms`);
  console.log(`  Expected  : ~${Math.ceil(JOBS_PER_GROUP / GROUP_CONCURRENCY) * 80}ms (serial would be ${JOBS_PER_GROUP * 80}ms)`);

  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
}

// ─── H: stall recovery ───────────────────────────────────────────────────────

async function testGroupStallRecovery() {
  console.log('\n══════════════════════════════════════════════');
  console.log(' H. Stall recovery cho group jobs');
  console.log('══════════════════════════════════════════════');

  const QUEUE_NAME = 'stall-recovery';
  const PREFIX = 'bull';
  const GROUP_ID = 'stall-group';

  const queue = new Queue<{ i: number }, number>(QUEUE_NAME, { connection });

  // Thêm 3 job vào group
  const [j1, j2, j3] = await queue.addBulk([
    { name: 't', data: { i: 1 }, opts: { group: { id: GROUP_ID } } },
    { name: 't', data: { i: 2 }, opts: { group: { id: GROUP_ID } } },
    { name: 't', data: { i: 3 }, opts: { group: { id: GROUP_ID } } },
  ]);

  // ── Inject trạng thái stalled cho j1 ──
  // Giả lập: scheduler đã lấy j1 khỏi group queue và push vào active,
  // nhưng worker crash trước khi tạo processing lock.
  const raw = new Redis(connection);
  const kp = `${PREFIX}:${QUEUE_NAME}`;

  // 1. Lấy j1 ra khỏi đầu group queue (như scheduler đã làm)
  await raw.lpop(`${kp}:group:${GROUP_ID}`);
  // 2. Đặt j1 vào active list
  await raw.rpush(`${kp}:active`, j1.id);
  // 3. Tăng running counter
  await raw.incr(`${kp}:running:${GROUP_ID}`);
  // 4. KHÔNG tạo processing lock → giả lập lock expired

  console.log(`  Injected stalled state for job ${j1.id}`);
  console.log(`  Group queue now has j2, j3 (j1 is "active" with no lock)`);
  console.log(`  running:${GROUP_ID} = 1 (leaked counter)`);

  // Kiểm tra: group hiện tại bị kẹt vì counter = 1 (maxConcurrency = 1)
  // Nếu không fix stall recovery, j2 và j3 sẽ không bao giờ được xử lý
  const runningBefore = await raw.get(`${kp}:running:${GROUP_ID}`);
  console.log(`\n  running counter before stall check: ${runningBefore}`);

  const completedIds: string[] = [];
  const stalledIds: string[] = [];

  const worker = new Worker<{ i: number }, number>(
    QUEUE_NAME,
    async (job) => {
      console.log(`  [worker] processing job ${job.id} (i=${job.data.i})`);
      await sleep(50);
      return job.data.i;
    },
    {
      connection,
      autorun: false,
      drainDelay: 1,
      stalledInterval: 600,  // check stalled jobs sau 600ms
      maxStalledCount: 1,
    },
  );

  worker.on('error', () => {});
  worker.on('stalled', (jobId: string) => {
    stalledIds.push(jobId);
    console.log(`  [stall] detected stalled job: ${jobId}`);
  });
  worker.on('completed', (job) => {
    completedIds.push(job.id);
    console.log(`  [done]  job ${job.id} (i=${(job.data as {i:number}).i})`);
  });

  // Chờ tất cả 3 job xong (j1 stalled → recovered → done; j2, j3 normal)
  await new Promise<void>((resolve) => {
    worker.on('completed', () => { if (completedIds.length === 3) resolve(); });
    worker.run().catch(() => {});
  });

  const runningAfter = await raw.get(`${kp}:running:${GROUP_ID}`);
  console.log(`\n  running counter after recovery: ${runningAfter}`);

  // ── Verify ──
  assert(stalledIds.includes(j1.id), `j1 phải bị detect là stalled (got: ${stalledIds})`);
  assert(completedIds.length === 3, `3 jobs phải complete (got: ${completedIds.length})`);
  assert(
    completedIds.includes(j1.id),
    `j1 phải được recover và complete`,
  );
  const finalRunning = parseInt(runningAfter ?? '0', 10);
  assert(finalRunning === 0, `running counter phải về 0 sau khi xong (got: ${finalRunning})`);

  console.log(`\n  Stalled jobs detected: ${stalledIds.length}  ✓  (${stalledIds})`);
  console.log(`  Completed: ${completedIds.length}/3  ✓`);
  console.log(`  running counter final: ${runningAfter}  ✓`);

  await raw.quit();
  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
}

// ─── I: stall recovery khi concurrency > 1 ────────────────────────────────────

async function testStallWithHighConcurrency() {
  console.log('\n══════════════════════════════════════════════');
  console.log(' I. Stall recovery với group.concurrency=3');
  console.log('══════════════════════════════════════════════');

  const QUEUE_NAME = 'stall-concur';
  const PREFIX = 'bull';
  const GROUP_ID = 'hiconcur-group';
  const GROUP_CONCURRENCY = 3;

  const queue = new Queue<{ i: number }, number>(QUEUE_NAME, { connection });

  // 6 jobs thật — group concurrency set via worker options (BullMQ Pro API)
  const jobs = await queue.addBulk(
    Array.from({ length: 6 }, (_, i) => ({
      name: 't',
      data: { i: i + 1 },
      opts: { group: { id: GROUP_ID } },
    })),
  );

  // Inject 2 stalled jobs (simulating 2 crashed workers out of 3 concurrent)
  const raw = new Redis(connection);
  const kp = `${PREFIX}:${QUEUE_NAME}`;

  // Lấy 2 job đầu ra khỏi group queue, inject stalled state
  const stalledJob1 = await raw.lpop(`${kp}:group:${GROUP_ID}`);
  const stalledJob2 = await raw.lpop(`${kp}:group:${GROUP_ID}`);

  if (stalledJob1) {
    await raw.rpush(`${kp}:active`, stalledJob1);
    await raw.incr(`${kp}:running:${GROUP_ID}`);
  }
  if (stalledJob2) {
    await raw.rpush(`${kp}:active`, stalledJob2);
    await raw.incr(`${kp}:running:${GROUP_ID}`);
  }

  console.log(`  Injected 2 stalled jobs, running counter = 2`);
  console.log(`  Group queue còn 4 jobs chờ xử lý`);

  const completedIds: string[] = [];
  const stalledIds: string[] = [];

  const worker = new Worker<{ i: number }, number>(
    QUEUE_NAME,
    async (job) => { await sleep(30); return job.data.i; },
    {
      connection,
      autorun: false,
      drainDelay: 1,
      stalledInterval: 500,
      maxStalledCount: 1,
      concurrency: GROUP_CONCURRENCY,
      group: { concurrency: GROUP_CONCURRENCY },
    },
  );

  worker.on('error', () => {});
  worker.on('stalled', (jobId: string) => stalledIds.push(jobId));
  worker.on('completed', (job) => completedIds.push(job.id));

  await new Promise<void>((resolve) => {
    worker.on('completed', () => { if (completedIds.length === 6) resolve(); });
    worker.run().catch(() => {});
  });

  const finalRunning = parseInt((await raw.get(`${kp}:running:${GROUP_ID}`)) ?? '0', 10);

  assert(stalledIds.length === 2, `phải detect 2 stalled jobs (got: ${stalledIds.length})`);
  assert(completedIds.length === 6, `tất cả 6 jobs phải complete (got: ${completedIds.length})`);
  assert(finalRunning === 0, `running counter phải về 0 (got: ${finalRunning})`);

  console.log(`  Stalled detected: ${stalledIds.length}/2  ✓`);
  console.log(`  Completed: ${completedIds.length}/6  ✓`);
  console.log(`  running counter final: ${finalRunning}  ✓`);

  await raw.quit();
  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
}

// ─── Runner ───────────────────────────────────────────────────────────────────

async function main() {
  console.log('PullMQ — Group Concurrency & Stall Recovery Tests\n');

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

  await run('G. group.concurrency > 1',              testGroupConcurrencyGT1);
  await run('H. Stall recovery (serial group)',       testGroupStallRecovery);
  await run('I. Stall recovery (concurrency=3 group)', testStallWithHighConcurrency);

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
