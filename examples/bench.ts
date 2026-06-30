/**
 * Throughput benchmark — jobs/second với processor rỗng (noop)
 *
 * Đo peak throughput của PullMQ ở các mức concurrency khác nhau.
 * Processor không làm gì → bottleneck hoàn toàn là overhead của thư viện + Redis.
 *
 * Run: npx tsx examples/bench.ts
 */
import Redis from 'ioredis';
import { Queue, Worker } from '../src';

const connection = { host: '127.0.0.1', port: 6379 };

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function fmt(n: number, width = 8) {
  return n.toFixed(0).padStart(width, ' ');
}

function fmtMs(ms: number) {
  return ms < 1000 ? `${ms.toFixed(0)}ms` : `${(ms / 1000).toFixed(2)}s`;
}

// ─── Single run ───────────────────────────────────────────────────────────────

async function runBench(opts: {
  label: string;
  jobs: number;
  concurrency: number;
  batchSize?: number;
  useGroups?: boolean;
  groupCount?: number;
  groupConcurrency?: number;
}): Promise<{ jobsPerSec: number; elapsedMs: number }> {
  const {
    label,
    jobs: TOTAL,
    concurrency,
    batchSize = 500,
    useGroups = false,
    groupCount = 10,
    groupConcurrency = 1,
  } = opts;

  const raw = new Redis(connection);
  await raw.flushall();
  await raw.quit();

  const queue = new Queue<void, void>('bench', { connection });

  // Pre-load tất cả jobs trước khi bắt worker
  const t_add = Date.now();
  for (let i = 0; i < TOTAL; i += batchSize) {
    const n = Math.min(batchSize, TOTAL - i);
    await queue.addBulk(
      Array.from({ length: n }, (_, k) => {
        const groupId = useGroups ? `g${(i + k) % groupCount}` : undefined;
        return {
          name: 'noop',
          data: undefined as void,
          opts: groupId
            ? { group: { id: groupId, concurrency: groupConcurrency } }
            : undefined,
        };
      }),
    );
  }
  const addMs = Date.now() - t_add;

  let done = 0;
  const worker = new Worker<void, void>(
    'bench',
    async () => { /* noop */ },
    { connection, autorun: false, drainDelay: 1, concurrency },
  );
  worker.on('error', () => {});

  const t0 = Date.now();
  await new Promise<void>((resolve) => {
    worker.on('completed', () => { if (++done === TOTAL) resolve(); });
    worker.run().catch(() => {});
  });
  const elapsedMs = Date.now() - t0;
  const jobsPerSec = Math.round(TOTAL / (elapsedMs / 1000));

  const addRate = Math.round(TOTAL / (addMs / 1000));

  console.log(
    `  ${label.padEnd(36)} │` +
    `${fmt(TOTAL)} jobs │` +
    `${fmt(concurrency, 5)} conc │` +
    ` ${fmtMs(elapsedMs).padStart(7)} │` +
    `${fmt(jobsPerSec, 8)} jobs/s │` +
    ` add=${fmt(addRate, 7)}/s`,
  );

  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();

  return { jobsPerSec, elapsedMs };
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log('PullMQ Throughput Benchmark');
  console.log('Processor: noop (measures pure framework + Redis overhead)\n');

  console.log(
    `  ${'scenario'.padEnd(36)} │` +
    `${'jobs'.padStart(8)} jobs │` +
    `${'conc'.padStart(5)} conc │` +
    ` ${'elapsed'.padStart(7)} │` +
    `${'jobs/s'.padStart(8)} jobs/s │` +
    ` ${'add/s'.padStart(7)}/s`,
  );
  console.log('  ' + '─'.repeat(100));

  // ── Bảng 1: Tăng concurrency, 5000 jobs ──────────────────────────────────
  console.log('\n  【 Tăng concurrency — 5000 jobs 】');

  const results5k: { conc: number; rps: number }[] = [];
  for (const conc of [1, 2, 5, 10, 20, 50, 100]) {
    const r = await runBench({ label: `concurrency=${conc}`, jobs: 5000, concurrency: conc });
    results5k.push({ conc, rps: r.jobsPerSec });
    await sleep(200);
  }

  // ── Bảng 2: Tăng số lượng jobs, concurrency=20 ───────────────────────────
  console.log('\n  【 Scale số lượng job — concurrency=20 】');

  for (const n of [1000, 5000, 10000, 20000]) {
    await runBench({ label: `${n} jobs`, jobs: n, concurrency: 20 });
    await sleep(200);
  }

  // ── Bảng 3: Group scheduling overhead ────────────────────────────────────
  console.log('\n  【 Group scheduling overhead — 5000 jobs, concurrency=20 】');

  await runBench({ label: 'no groups (baseline)',    jobs: 5000, concurrency: 20 });
  await runBench({ label: 'groups=10  concur/g=1',  jobs: 5000, concurrency: 20, useGroups: true, groupCount: 10,  groupConcurrency: 1 });
  await runBench({ label: 'groups=50  concur/g=1',  jobs: 5000, concurrency: 20, useGroups: true, groupCount: 50,  groupConcurrency: 1 });
  await runBench({ label: 'groups=100 concur/g=1',  jobs: 5000, concurrency: 20, useGroups: true, groupCount: 100, groupConcurrency: 1 });
  await runBench({ label: 'groups=10  concur/g=5',  jobs: 5000, concurrency: 20, useGroups: true, groupCount: 10,  groupConcurrency: 5 });
  await runBench({ label: 'groups=50  concur/g=5',  jobs: 5000, concurrency: 20, useGroups: true, groupCount: 50,  groupConcurrency: 5 });

  // ── Summary ───────────────────────────────────────────────────────────────
  const peak = results5k.reduce((a, b) => (b.rps > a.rps ? b : a));
  const single = results5k.find((r) => r.conc === 1)!;

  console.log('\n' + '═'.repeat(70));
  console.log(` Peak throughput  : ${peak.rps.toLocaleString()} jobs/s  (concurrency=${peak.conc})`);
  console.log(` Single-threaded  : ${single.rps.toLocaleString()} jobs/s  (concurrency=1)`);
  console.log(` Speedup at peak  : ${(peak.rps / single.rps).toFixed(1)}×`);
  console.log('═'.repeat(70));

  process.exit(0);
}

main().catch((err) => { console.error(err); process.exit(1); });
