/**
 * Example: demonstrates PullMQ API compatibility with BullMQ
 * Run: npx tsx src/example.ts
 *
 * Requires a local Redis instance on 127.0.0.1:6379
 */
import { Queue, Worker, QueueEvents } from './index';

const connection = { host: '127.0.0.1', port: 6379 };

// ─── Example 1: Basic — add, priority, delay ──────────────────────────────────

async function runBasicExample() {
  console.log('=== Basic Queue/Worker Example ===');

  const queue = new Queue<{ name: string }, string>('basic-queue', { connection });
  const events = new QueueEvents('basic-queue', { connection });

  events.on('completed', ({ jobId, returnvalue }: { jobId: string; returnvalue: string }) => {
    console.log(`[event] completed jobId=${jobId} result=${returnvalue}`);
  });
  events.run().catch(() => {});

  const worker = new Worker<{ name: string }, string>(
    'basic-queue',
    async (job) => {
      console.log(`[worker] processing job ${job.id}: hello, ${job.data.name}`);
      await sleep(80);
      return `Hello, ${job.data.name}!`;
    },
    { connection, autorun: false, drainDelay: 1 },
  );

  worker.on('error', () => {/* suppress */});
  worker.on('completed', (job, result) =>
    console.log(`  ✓ ${job.id} → ${result}`));

  // Add: 1 normal, 1 delayed (500ms), 1 priority
  const j1 = await queue.add('greet', { name: 'Alice' });
  const j2 = await queue.add('greet', { name: 'Bob' }, { delay: 500 });
  const j3 = await queue.add('greet', { name: 'Charlie' }, { priority: 1 });

  console.log(`Added: ${j1.id} (normal), ${j2.id} (delayed), ${j3.id} (priority=1)`);
  console.log('Counts before:', await queue.getJobCounts());

  // Wait until all 3 complete
  let completedCount = 0;
  await new Promise<void>((resolve) => {
    worker.on('completed', () => { if (++completedCount === 3) resolve(); });
    worker.run().catch(() => {});
  });

  console.log('Counts after: ', await queue.getJobCounts());

  await worker.close();
  await events.close();
  await queue.obliterate({ force: true });
  await queue.close();
  console.log('Done.\n');
}

// ─── Example 2: Group scheduling (round-robin) ────────────────────────────────

async function runGroupExample() {
  console.log('=== Group Scheduling Example (round-robin) ===');

  const queue = new Queue<{ task: string }, void>('group-queue', { connection });

  const processedOrder: string[] = [];

  const worker = new Worker<{ task: string }, void>(
    'group-queue',
    async (job) => {
      const label = `group=${job.opts.group?.id} task=${job.data.task}`;
      console.log(`[worker] ${label}`);
      processedOrder.push(job.data.task);
      await sleep(50);
    },
    { connection, autorun: false, drainDelay: 1 },
  );

  worker.on('error', () => {/* suppress */});
  worker.on('completed', (job) =>
    console.log(`  ✓ ${job.id} (group: ${job.opts.group?.id})`));

  // Enqueue 3 jobs per group
  for (let i = 1; i <= 3; i++) {
    await queue.add('task', { task: `A-${i}` }, { group: { id: 'groupA' } });
  }
  for (let i = 1; i <= 3; i++) {
    await queue.add('task', { task: `B-${i}` }, { group: { id: 'groupB' } });
  }

  console.log('Enqueued 3 jobs in groupA and 3 in groupB');

  let completedCount = 0;
  await new Promise<void>((resolve) => {
    worker.on('completed', () => { if (++completedCount === 6) resolve(); });
    worker.run().catch(() => {});
  });

  console.log('Processing order:', processedOrder.join(', '));
  console.log('(groups should interleave: A-1, B-1, A-2, B-2, ...)');

  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
  console.log('Done.\n');
}

// ─── Example 3: Retry with backoff ───────────────────────────────────────────

async function runRetryExample() {
  console.log('=== Retry Example (3 attempts, fixed backoff 200ms) ===');

  const queue = new Queue<{ n: number }, string>('retry-queue', { connection });
  let calls = 0;

  const worker = new Worker<{ n: number }, string>(
    'retry-queue',
    async (job) => {
      calls++;
      console.log(`[worker] attempt ${job.attemptsMade}/${job.opts.attempts}`);
      if (job.attemptsMade < 3) {
        throw new Error(`fail on attempt ${job.attemptsMade}`);
      }
      return 'success!';
    },
    { connection, autorun: false, drainDelay: 1 },
  );

  worker.on('error', () => {/* suppress */});
  worker.on('failed', (job, err) =>
    console.log(`  ✗ attempt ${job?.attemptsMade}: ${err.message}`));

  await queue.add('retry-test', { n: 1 }, { attempts: 3, backoff: { type: 'fixed', delay: 200 } });

  let done = false;
  await new Promise<void>((resolve) => {
    worker.on('completed', (job, result) => {
      console.log(`  ✓ completed: ${result}`);
      done = true;
      resolve();
    });
    worker.run().catch(() => {});
  });

  console.log(`Total processor calls: ${calls}`);
  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
  console.log('Done.\n');
}

// ─── Example 4: Rate limit awareness ─────────────────────────────────────────

async function runRateLimitExample() {
  console.log('=== getJobCounts / clean Example ===');

  const queue = new Queue('stats-queue', { connection });

  for (let i = 0; i < 5; i++) {
    await queue.add('job', { i });
  }

  const worker = new Worker('stats-queue', async (job) => {
    if ((job.data as { i: number }).i % 2 === 0) throw new Error('even jobs fail');
    return 'ok';
  }, { connection, autorun: false, drainDelay: 1 });

  worker.on('error', () => {});

  let done = 0;
  await new Promise<void>((resolve) => {
    worker.on('completed', () => { if (++done === 5) resolve(); });
    worker.on('failed', () => { if (++done === 5) resolve(); });
    worker.run().catch(() => {});
  });

  const counts = await queue.getJobCounts();
  console.log('Final counts:', counts);

  // Retry all failed jobs
  await queue.retryJobs({ state: 'failed' });
  console.log('Counts after retry:', await queue.getJobCounts());

  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
  console.log('Done.\n');
}

// ─── Runner ───────────────────────────────────────────────────────────────────

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  await runBasicExample();
  await runGroupExample();
  await runRetryExample();
  await runRateLimitExample();
  process.exit(0);
}

main().catch((err) => {
  console.error('Fatal:', err);
  process.exit(1);
});
