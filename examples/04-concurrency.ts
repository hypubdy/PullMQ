/**
 * Example 4: Concurrency — nhiều worker, pause/resume, job counts
 *
 * Minh họa:
 * - concurrency: N jobs xử lý đồng thời trong 1 worker
 * - Nhiều Worker instance cùng consume 1 queue
 * - pause() / resume() queue
 * - addBulk() thêm nhiều job một lần
 * - getJobs() lấy danh sách job theo state
 *
 * Run: npx tsx examples/04-concurrency.ts
 */
import { Queue, Worker } from '../src';

const connection = { host: '127.0.0.1', port: 6379 };

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function runConcurrencyDemo() {
  console.log('--- concurrency=3: 6 jobs xử lý song song theo nhóm 3 ---');

  const queue = new Queue<{ n: number }, number>('concurrency-demo', { connection });
  const startTimes: Record<string, number> = {};
  const endTimes: Record<string, number> = {};

  const worker = new Worker<{ n: number }, number>(
    'concurrency-demo',
    async (job) => {
      startTimes[job.id] = Date.now();
      console.log(`  [start] job-${job.data.n}`);
      await sleep(300); // mỗi job mất 300ms
      endTimes[job.id] = Date.now();
      console.log(`  [done]  job-${job.data.n}`);
      return job.data.n * 2;
    },
    { connection, autorun: false, drainDelay: 1, concurrency: 3 },
  );

  worker.on('error', () => {});

  const start = Date.now();
  await queue.addBulk([
    { name: 'compute', data: { n: 1 } },
    { name: 'compute', data: { n: 2 } },
    { name: 'compute', data: { n: 3 } },
    { name: 'compute', data: { n: 4 } },
    { name: 'compute', data: { n: 5 } },
    { name: 'compute', data: { n: 6 } },
  ]);

  console.log('Added 6 jobs via addBulk()\n');

  let done = 0;
  await new Promise<void>((resolve) => {
    worker.on('completed', () => { if (++done === 6) resolve(); });
    worker.run().catch(() => {});
  });

  const elapsed = Date.now() - start;
  console.log(`\nTotal time: ${elapsed}ms`);
  console.log('With concurrency=3 and 300ms/job: expect ~600ms (2 batches of 3)');

  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
}

async function runMultiWorkerDemo() {
  console.log('\n--- 2 Workers cùng consume 1 queue ---');

  const queue = new Queue<{ n: number }, void>('multi-worker', { connection });
  const processedBy: Record<string, string> = {};

  const makeWorker = (id: string) => {
    const w = new Worker<{ n: number }, void>(
      'multi-worker',
      async (job) => {
        processedBy[`job-${job.data.n}`] = id;
        console.log(`  [${id}] processing job-${job.data.n}`);
        await sleep(100);
      },
      { connection, autorun: false, drainDelay: 1, concurrency: 1 },
    );
    w.on('error', () => {});
    return w;
  };

  const w1 = makeWorker('worker-A');
  const w2 = makeWorker('worker-B');

  await queue.addBulk(
    Array.from({ length: 6 }, (_, i) => ({ name: 'task', data: { n: i + 1 } })),
  );

  console.log('Added 6 jobs, starting 2 workers...\n');

  let done = 0;
  await new Promise<void>((resolve) => {
    const onComplete = () => { if (++done === 6) resolve(); };
    w1.on('completed', onComplete);
    w2.on('completed', onComplete);
    w1.run().catch(() => {});
    w2.run().catch(() => {});
  });

  console.log('\nProcessed by:');
  for (const [job, worker] of Object.entries(processedBy)) {
    console.log(`  ${job} → ${worker}`);
  }
  const byWorker = Object.values(processedBy).reduce<Record<string, number>>((acc, w) => {
    acc[w] = (acc[w] ?? 0) + 1;
    return acc;
  }, {});
  console.log('Distribution:', byWorker);

  await w1.close();
  await w2.close();
  await queue.obliterate({ force: true });
  await queue.close();
}

async function runPauseResumeDemo() {
  console.log('\n--- pause() / resume() ---');

  const queue = new Queue<{ n: number }, void>('pause-demo', { connection });

  const worker = new Worker<{ n: number }, void>(
    'pause-demo',
    async (job) => {
      console.log(`  processing job-${job.data.n}`);
      await sleep(80);
    },
    { connection, autorun: false, drainDelay: 1 },
  );

  worker.on('error', () => {});

  // Add jobs upfront
  await queue.addBulk([
    { name: 'task', data: { n: 1 } },
    { name: 'task', data: { n: 2 } },
    { name: 'task', data: { n: 3 } },
  ]);

  console.log('Counts before pause:', await queue.getJobCounts('waiting'));

  // Pause queue, start worker — should NOT process anything
  await queue.pause();
  console.log('Queue paused. isPaused():', await queue.isPaused());

  let processedWhilePaused = 0;
  worker.on('completed', () => processedWhilePaused++);
  worker.run().catch(() => {});

  await sleep(400); // wait a bit
  console.log(`Processed while paused: ${processedWhilePaused} (expected 0)`);

  // Resume — worker picks up immediately
  await queue.resume();
  console.log('Queue resumed. isPaused():', await queue.isPaused());

  let done = 0;
  await new Promise<void>((resolve) => {
    worker.on('completed', () => { if (++done === 3) resolve(); });
  });

  console.log('All 3 jobs completed after resume.');
  console.log('Final counts:', await queue.getJobCounts());

  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
}

async function runGetJobsDemo() {
  console.log('\n--- getJobs() / getJob() ---');

  const queue = new Queue<{ payload: string }, string>('getjobs-demo', { connection });

  const jobs = await queue.addBulk([
    { name: 'a', data: { payload: 'hello' } },
    { name: 'b', data: { payload: 'world' } },
    { name: 'c', data: { payload: 'foo' }, opts: { delay: 60000 } },
  ]);

  const [j1] = jobs;
  console.log(`\nFetching job by ID: ${j1.id}`);
  const fetched = await queue.getJob(j1.id);
  console.log(`  name=${fetched?.name} data=${JSON.stringify(fetched?.data)}`);

  const waiting = await queue.getJobs('waiting');
  const delayed = await queue.getJobs('delayed');
  console.log(`\nWaiting jobs (${waiting.length}):`, waiting.map((j) => j.name));
  console.log(`Delayed jobs (${delayed.length}):`, delayed.map((j) => j.name));

  // Process waiting ones
  const worker = new Worker<{ payload: string }, string>(
    'getjobs-demo',
    async (job) => job.data.payload.toUpperCase(),
    { connection, autorun: false, drainDelay: 1 },
  );
  worker.on('error', () => {});

  let done = 0;
  await new Promise<void>((resolve) => {
    worker.on('completed', () => { if (++done === 2) resolve(); });
    worker.run().catch(() => {});
  });

  const completed = await queue.getJobs('completed');
  console.log(`\nCompleted jobs (${completed.length}):`, completed.map((j) => `${j.name}→${j.returnvalue}`));

  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
}

async function main() {
  await runConcurrencyDemo();
  await runMultiWorkerDemo();
  await runPauseResumeDemo();
  await runGetJobsDemo();
  console.log('\nAll concurrency examples done.');
  process.exit(0);
}

main().catch((err) => { console.error(err); process.exit(1); });
