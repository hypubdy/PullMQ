import type Redis from 'ioredis';
import { Job } from './job';
import { scripted } from './scripts';

// Single source of truth for promoting due delayed jobs. Both the Worker's
// periodic promoter and Queue.promoteJobs() delegate here, so claim semantics,
// group/priority routing and maxSize enforcement can never drift apart between
// the two paths again (that drift is exactly how ISSUES.md #1 happened).
//
// Each job is claimed AND routed by one atomic Lua call (pmqPromoteJob): the
// ZREM inside the script is the claim gate against concurrent promoters, and
// because claim and enqueue are a single script, a process killed mid-
// promotion can no longer lose the job (found by the kill-soak test).
//
// Returns the ids that were actually promoted (claimed AND enqueued). Jobs
// deferred because their group is at maxSize are not included.
export async function promoteDueDelayedJobs(
  client: Redis,
  queueName: string,
  prefix: string,
  opts: { limit?: number; deferMs?: number } = {},
): Promise<string[]> {
  const keyPrefix = `${prefix}:${queueName}`;
  const now = Date.now();

  const ids = opts.limit !== undefined
    ? await client.zrangebyscore(`${keyPrefix}:delayed`, '-inf', now, 'LIMIT', 0, opts.limit)
    : await client.zrangebyscore(`${keyPrefix}:delayed`, '-inf', now);
  if (!ids.length) return [];

  const promoted: string[] = [];
  for (const id of ids) {
    // Read the hash BEFORE claiming — routing needs the job's group/priority
    // opts. If another promoter claims the job in the meantime, the script's
    // ZREM gate returns 0 and this read is simply discarded.
    const job = await Job.fromId(client, queueName, prefix, id);
    if (!job) {
      // Orphan id whose hash is gone (removed/evicted) — drop the entry.
      await client.zrem(`${keyPrefix}:delayed`, id);
      continue;
    }

    const groupId = job.opts.group?.id ?? '';
    const result = await scripted(client).pmqPromoteJob(
      `${keyPrefix}:delayed`,
      `${keyPrefix}:group:${groupId}`,
      `${keyPrefix}:group:priority:${groupId}`,
      `${keyPrefix}:groups:set`,
      `${keyPrefix}:groups:active`,
      `${keyPrefix}:priority`,
      `${keyPrefix}:ready`,
      id,
      groupId,
      String(job.opts.group?.priority ?? 0),
      String(job.opts.priority ?? 0),
      // A delayed job never went through Queue.add()'s maxSize check (it was
      // diverted straight to :delayed before the group branch), so the script
      // enforces it at promotion time and defers (return 2) when full.
      String(job.opts.group?.maxSize ?? -1),
      String(Date.now() + (opts.deferMs ?? 1000)),
    );
    if (result === 1) promoted.push(id);
  }
  return promoted;
}
