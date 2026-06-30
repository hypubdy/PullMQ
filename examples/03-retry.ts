/**
 * Example 3: Retry — backoff strategies & UnrecoverableError
 *
 * Minh họa:
 * - Fixed backoff: chờ đúng N ms giữa các lần thử
 * - Exponential backoff: delay tăng gấp đôi mỗi lần
 * - UnrecoverableError: dừng retry ngay, không chờ hết attempts
 * - retryJobs(): retry thủ công từ dashboard
 *
 * Run: npx tsx examples/03-retry.ts
 */
import { Queue, Worker, UnrecoverableError } from '../src';

const connection = { host: '127.0.0.1', port: 6379 };

async function runFixed() {
  console.log('--- Fixed backoff (200ms, 3 attempts) ---');
  const queue = new Queue<{ id: string }, string>('retry-fixed', { connection });
  const timeline: string[] = [];

  const worker = new Worker<{ id: string }, string>(
    'retry-fixed',
    async (job) => {
      const ts = Date.now();
      timeline.push(`attempt ${job.attemptsMade} @ ${ts}`);
      console.log(`  attempt ${job.attemptsMade}/${job.opts.attempts} for ${job.data.id}`);
      if (job.attemptsMade < 3) throw new Error(`transient error`);
      return 'ok';
    },
    { connection, autorun: false, drainDelay: 1 },
  );

  worker.on('error', () => {});
  worker.on('failed', (job, err) =>
    console.log(`  ✗ attempt ${job?.attemptsMade} failed: ${err.message}`));

  const start = Date.now();
  await queue.add('task', { id: 'fixed-001' }, {
    attempts: 3,
    backoff: { type: 'fixed', delay: 200 },
  });

  await new Promise<void>((resolve) => {
    worker.on('completed', (job, result) => {
      const elapsed = Date.now() - start;
      console.log(`  ✓ completed in ${elapsed}ms result="${result}"`);
      resolve();
    });
    worker.run().catch(() => {});
  });

  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
}

async function runExponential() {
  console.log('\n--- Exponential backoff (base 100ms, 4 attempts) ---');
  const queue = new Queue<{ id: string }, string>('retry-exp', { connection });

  const worker = new Worker<{ id: string }, string>(
    'retry-exp',
    async (job) => {
      console.log(`  attempt ${job.attemptsMade}/${job.opts.attempts}`);
      // delays: 100ms, 200ms, 400ms, then success
      if (job.attemptsMade < 4) throw new Error('exponential failure');
      return 'recovered';
    },
    { connection, autorun: false, drainDelay: 1 },
  );

  worker.on('error', () => {});
  worker.on('failed', (job, err) =>
    console.log(`  ✗ attempt ${job?.attemptsMade}: ${err.message}`));

  await queue.add('task', { id: 'exp-001' }, {
    attempts: 4,
    backoff: { type: 'exponential', delay: 100 },
  });

  await new Promise<void>((resolve) => {
    worker.on('completed', (_, result) => {
      console.log(`  ✓ completed result="${result}"`);
      resolve();
    });
    worker.run().catch(() => {});
  });

  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
}

async function runUnrecoverable() {
  console.log('\n--- UnrecoverableError (dừng retry ngay dù còn attempts) ---');
  const queue = new Queue<{ id: string }, void>('retry-unrecoverable', { connection });

  const worker = new Worker<{ id: string }, void>(
    'retry-unrecoverable',
    async (job) => {
      console.log(`  attempt ${job.attemptsMade}/${job.opts.attempts}`);
      throw new UnrecoverableError('data validation failed — no point retrying');
    },
    { connection, autorun: false, drainDelay: 1 },
  );

  worker.on('error', () => {});

  await queue.add('validate', { id: 'bad-data' }, { attempts: 10 });

  await new Promise<void>((resolve) => {
    worker.on('failed', (job, err) => {
      console.log(`  ✗ failed immediately: ${err.message}`);
      console.log(`    attemptsMade=${job?.attemptsMade} (expected 1, not 10)`);
      resolve();
    });
    worker.run().catch(() => {});
  });

  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
}

async function runManualRetry() {
  console.log('\n--- Manual retry via queue.retryJobs() ---');
  const queue = new Queue<{ id: string }, string>('retry-manual', { connection });
  let callCount = 0;

  const worker = new Worker<{ id: string }, string>(
    'retry-manual',
    async (job) => {
      callCount++;
      // First run: always fail. After manual retry: succeed.
      if (callCount === 1) throw new Error('first run fails');
      return `success on call #${callCount}`;
    },
    { connection, autorun: false, drainDelay: 1 },
  );

  worker.on('error', () => {});

  await queue.add('task', { id: 'manual-retry' }, { attempts: 1 });

  // Wait for initial failure
  await new Promise<void>((resolve) => {
    worker.on('failed', (job, err) => {
      console.log(`  ✗ failed: ${err.message}`);
      resolve();
    });
    worker.run().catch(() => {});
  });

  console.log('  Counts after failure:', await queue.getJobCounts('failed', 'waiting'));

  // Manually retry all failed jobs
  await queue.retryJobs({ state: 'failed' });
  console.log('  Counts after retryJobs():', await queue.getJobCounts('failed', 'waiting'));

  await new Promise<void>((resolve) => {
    worker.on('completed', (_, result) => {
      console.log(`  ✓ ${result}`);
      resolve();
    });
  });

  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
}

async function main() {
  await runFixed();
  await runExponential();
  await runUnrecoverable();
  await runManualRetry();
  console.log('\nAll retry examples done.');
  process.exit(0);
}

main().catch((err) => { console.error(err); process.exit(1); });
