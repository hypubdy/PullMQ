/**
 * Example 1: Basic — add, priority, delay, events
 *
 * Minh họa:
 * - Thêm job thường, job delay, job priority
 * - Priority job được xử lý trước
 * - Delay job chờ đủ thời gian mới chạy
 * - QueueEvents lắng nghe sự kiện real-time
 *
 * Run: npx tsx examples/01-basic.ts
 */
import { Queue, Worker, QueueEvents } from '../src';

const connection = { host: '127.0.0.1', port: 6379 };

interface EmailJob {
  to: string;
  subject: string;
}

async function main() {
  const queue = new Queue<EmailJob, string>('emails', { connection });
  const events = new QueueEvents('emails', { connection });

  // Lắng nghe sự kiện từ Redis Streams
  events.on('added', ({ jobId, name }: { jobId: string; name: string }) =>
    console.log(`[event] added    jobId=${jobId} name=${name}`),
  );
  events.on('active', ({ jobId }: { jobId: string }) =>
    console.log(`[event] active   jobId=${jobId}`),
  );
  events.on('completed', ({ jobId, returnvalue }: { jobId: string; returnvalue: string }) =>
    console.log(`[event] completed jobId=${jobId} → ${returnvalue}`),
  );
  events.run().catch(() => {});

  const worker = new Worker<EmailJob, string>(
    'emails',
    async (job) => {
      console.log(`[worker] sending "${job.name}" to ${job.data.to} (attempt ${job.attemptsMade})`);
      await sleep(120);
      return `sent:${job.id}`;
    },
    { connection, autorun: false, drainDelay: 1 },
  );

  worker.on('error', () => {});

  // --- Thêm jobs ---
  const urgent = await queue.add(
    'welcome',
    { to: 'vip@example.com', subject: 'Welcome VIP!' },
    { priority: 1 },                // ưu tiên cao nhất
  );
  const normal = await queue.add(
    'newsletter',
    { to: 'user@example.com', subject: 'Monthly digest' },
  );
  const scheduled = await queue.add(
    'reminder',
    { to: 'late@example.com', subject: 'Reminder' },
    { delay: 800 },                  // chạy sau 800ms
  );

  console.log(`\nAdded jobs:`);
  console.log(`  ${urgent.id}   ← priority=1 (chạy trước)`);
  console.log(`  ${normal.id}   ← normal`);
  console.log(`  ${scheduled.id}   ← delayed 800ms\n`);

  console.log('Queue counts:', await queue.getJobCounts('waiting', 'delayed'));

  // --- Chạy worker, chờ cả 3 job xong ---
  let done = 0;
  await new Promise<void>((resolve) => {
    worker.on('completed', (job, result) => {
      console.log(`  ✓ ${job.name} [${job.id}] result="${result}"`);
      if (++done === 3) resolve();
    });
    worker.run().catch(() => {});
  });

  console.log('\nFinal counts:', await queue.getJobCounts());

  // --- Cleanup ---
  await worker.close();
  await events.close();
  await queue.obliterate({ force: true });
  await queue.close();
  console.log('\nDone.');
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

main().catch((err) => { console.error(err); process.exit(1); });
