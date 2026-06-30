/**
 * Example 2: Group scheduling — round-robin per contact
 *
 * Minh họa mô hình thực tế: gửi email automation cho CRM
 * - tenant1 có 2 contact (A, B), mỗi contact có 3 bước
 * - tenant2 có 1 contact (C) với 2 bước
 * - Mỗi contact là 1 group → các bước trong group chạy FIFO, tuần tự
 * - Nhiều group chạy round-robin (xen kẽ nhau)
 * - Bảo đảm contact A không bị vượt bởi contact B và ngược lại
 *
 * Run: npx tsx examples/02-groups.ts
 */
import { Queue, Worker } from '../src';

const connection = { host: '127.0.0.1', port: 6379 };

interface StepJob {
  contactId: string;
  step: string;
  message: string;
}

async function main() {
  const queue = new Queue<StepJob, void>('crm-steps', { connection });

  const log: string[] = [];

  const worker = new Worker<StepJob, void>(
    'crm-steps',
    async (job) => {
      const tag = `[${job.opts.group!.id}] ${job.data.step}`;
      console.log(`  processing  ${tag}`);
      log.push(tag);
      await sleep(60);
    },
    { connection, autorun: false, drainDelay: 1 },
  );

  worker.on('error', () => {});

  // --- Enqueue: tenant1/contactA (3 steps) ---
  const groupA = { id: 'tenant1:contactA' };
  await queue.add('send-email',    { contactId: 'A', step: '1-welcome',   message: 'Hi Alice!' }, { group: groupA });
  await queue.add('wait-reply',    { contactId: 'A', step: '2-follow-up', message: 'Did you get it?' }, { group: groupA });
  await queue.add('send-discount', { contactId: 'A', step: '3-offer',     message: '20% off!' }, { group: groupA });

  // --- Enqueue: tenant1/contactB (3 steps) ---
  const groupB = { id: 'tenant1:contactB' };
  await queue.add('send-email',    { contactId: 'B', step: '1-welcome',   message: 'Hi Bob!' },   { group: groupB });
  await queue.add('wait-reply',    { contactId: 'B', step: '2-follow-up', message: 'Any questions?' }, { group: groupB });
  await queue.add('send-discount', { contactId: 'B', step: '3-offer',     message: '15% off!' }, { group: groupB });

  // --- Enqueue: tenant2/contactC (2 steps) ---
  const groupC = { id: 'tenant2:contactC' };
  await queue.add('send-email',    { contactId: 'C', step: '1-welcome',   message: 'Hi Carol!' }, { group: groupC });
  await queue.add('wait-reply',    { contactId: 'C', step: '2-follow-up', message: 'Need help?' }, { group: groupC });

  console.log('Enqueued 8 steps across 3 contacts (groups).\n');
  console.log('Expected: round-robin across groups, FIFO within each group.\n');

  // --- Chạy worker, chờ 8 job xong ---
  let done = 0;
  const total = 8;

  await new Promise<void>((resolve) => {
    worker.on('completed', () => { if (++done === total) resolve(); });
    worker.run().catch(() => {});
  });

  // --- Kết quả ---
  console.log('\n=== Processing order ===');
  log.forEach((l, i) => console.log(`  ${i + 1}. ${l}`));

  // Kiểm tra FIFO trong mỗi group
  const byGroup: Record<string, string[]> = {};
  for (const entry of log) {
    const [group, step] = entry.replace('[', '').split('] ');
    (byGroup[group] ??= []).push(step);
  }
  console.log('\n=== Order within each group (must be FIFO) ===');
  for (const [g, steps] of Object.entries(byGroup)) {
    console.log(`  ${g}: ${steps.join(' → ')}`);
  }

  // --- Cleanup ---
  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
  console.log('\nDone.');
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

main().catch((err) => { console.error(err); process.exit(1); });
