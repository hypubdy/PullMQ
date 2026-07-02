/**
 * Soak test: multiple worker PROCESSES under random SIGKILL.
 *
 * Verifies the crash-safety invariants that unit tests cannot:
 *   1. No job is ever lost — every added job ends up completed, failed, or
 *      still pending; none vanish from all structures at once.
 *   2. No group stalls forever — the queue fully drains after producers stop.
 *   3. All running:{group} counters return to 0 (never negative).
 *   4. group:job-map is empty at the end (no leaked dispatch entries).
 *
 * Duplicate executions ARE allowed (at-least-once semantics: a worker killed
 * mid-job causes a stalled re-run) — they are reported, not asserted.
 *
 * Run: npx tsx examples/06-soak-kill.ts [durationSec] [numWorkers]
 */
import { spawn, type ChildProcess } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import Redis from 'ioredis';
import { Queue, Worker } from '../src';

const connection = { host: '127.0.0.1', port: 6379 };
const QUEUE = 'soak';
const KP = `bull:${QUEUE}`;
const EXEC_KEY = 'soakacct:executions'; // outside the bull: prefix on purpose
const GROUPS = 10;

type Kind = 'plain' | 'group' | 'gpriority' | 'priority' | 'delayed' | 'fail' | 'hardfail';
interface JobData { kind: Kind; i: number }

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const selfPath = fileURLToPath(import.meta.url);
// Spawn workers as a DIRECT node process with tsx's loader flags. Spawning the
// tsx CLI instead puts a supervisor process in between: SIGKILL then hits the
// supervisor while the real worker survives as an orphan — the kills never
// actually kill anything.
const tsxDist = path.resolve(path.dirname(selfPath), '..', 'node_modules', 'tsx', 'dist');
const workerSpawnArgs = [
  '--require', path.join(tsxDist, 'preflight.cjs'),
  '--import', pathToFileURL(path.join(tsxDist, 'loader.mjs')).href,
  selfPath, '--worker',
];
const workerLogPath = path.join(os.tmpdir(), 'pullmq-soak-workers.log');

if (process.argv[2] === '--worker') {
  runWorker();
} else {
  orchestrate().catch((e) => { console.error(e); process.exit(1); });
}

// ─── Worker child process ─────────────────────────────────────────────────────

async function runWorker(): Promise<void> {
  const exec = new Redis(connection);
  const worker = new Worker<JobData, void>(
    QUEUE,
    async (job) => {
      await exec.hincrby(EXEC_KEY, job.id, 1);
      if (job.data.kind === 'hardfail') throw new Error('hard failure (planned)');
      if (job.data.kind === 'fail' && job.attemptsMade < 2) throw new Error('transient failure (planned)');
      await sleep(5 + Math.random() * 25);
    },
    {
      connection,
      concurrency: 4,
      drainDelay: 1,
      // Short lock/stall cycle so jobs orphaned by SIGKILL are reclaimed fast
      // enough for a minutes-long soak.
      lockDuration: 3000,
      stalledInterval: 1500,
      maxStalledCount: 5,
      group: { concurrency: 2 },
    },
  );
  worker.on('error', () => {/* redis hiccups during churn are expected */});
  // Process stays alive until the orchestrator kills it.
}

// ─── Orchestrator ─────────────────────────────────────────────────────────────

async function orchestrate(): Promise<void> {
  const DURATION_MS = (Number(process.argv[2]) || 180) * 1000;
  const NUM_WORKERS = Number(process.argv[3]) || 4;

  const redis = new Redis(connection);
  await redis.flushall();

  const queue = new Queue<JobData, void>(QUEUE, { connection });
  queue.on('error', (err) => console.error('queue error:', err));
  const addedKind = new Map<string, Kind>();
  let kills = 0;
  let respawns = 0;

  // ── Worker fleet ────────────────────────────────────────────────────────────
  const workers: Array<{ proc: ChildProcess; alive: boolean }> = [];
  const allProcs: ChildProcess[] = [];
  const workerLogFd = fs.openSync(workerLogPath, 'a');

  function killEverything(): void {
    for (const p of allProcs) { try { p.kill('SIGKILL'); } catch { /* gone */ } }
  }
  process.on('unhandledRejection', (err) => {
    console.error('UNHANDLED REJECTION:', err);
    killEverything();
    process.exit(1);
  });
  // Hard watchdog: whatever happens, never leave orphans behind.
  const watchdog = setTimeout(() => {
    console.error('WATCHDOG: soak exceeded its deadline — aborting.');
    killEverything();
    process.exit(1);
  }, DURATION_MS + 300_000);
  watchdog.unref();

  function spawnWorker(slot: number): void {
    const proc = spawn(process.execPath, workerSpawnArgs, {
      stdio: ['ignore', 'ignore', workerLogFd],
    });
    workers[slot] = { proc, alive: true };
    allProcs.push(proc);
    proc.on('exit', () => { if (workers[slot]?.proc === proc) workers[slot].alive = false; });
  }
  for (let i = 0; i < NUM_WORKERS; i++) spawnWorker(i);

  // ── Killer: SIGKILL a random worker every 2–4s, respawn 300ms later ────────
  let killing = true;
  const killerLoop = (async () => {
    while (killing) {
      await sleep(2000 + Math.random() * 2000);
      if (!killing) break;
      const slot = Math.floor(Math.random() * NUM_WORKERS);
      const w = workers[slot];
      if (w?.alive) {
        w.proc.kill('SIGKILL');
        kills++;
        await sleep(300);
        if (killing) { spawnWorker(slot); respawns++; }
      }
    }
  })();

  // ── Producer: ~200 jobs/s of mixed shapes ──────────────────────────────────
  let producing = true;
  let batchNo = 0;
  const producerLoop = (async () => {
    while (producing) {
      const batch: Array<{ name: string; data: JobData; opts?: object }> = [];
      const mk = (kind: Kind, opts?: object) => batch.push({ name: kind, data: { kind, i: batchNo }, opts });

      for (let k = 0; k < 10; k++) mk('group', { group: { id: `g${Math.floor(Math.random() * GROUPS)}` } });
      for (let k = 0; k < 2; k++) mk('gpriority', { group: { id: `g${Math.floor(Math.random() * GROUPS)}`, priority: 1 + Math.floor(Math.random() * 3) } });
      for (let k = 0; k < 3; k++) mk('plain');
      for (let k = 0; k < 2; k++) mk('priority', { priority: 1 + Math.floor(Math.random() * 5) });
      for (let k = 0; k < 2; k++) mk('delayed', { delay: 50 + Math.floor(Math.random() * 450) });
      if (batchNo % 5 === 0) mk('hardfail', { attempts: 1 });
      else mk('fail', { attempts: 2, backoff: { type: 'fixed', delay: 100 } });

      try {
        const jobs = await queue.addBulk(batch as never);
        for (const j of jobs) addedKind.set(j.id, (j.data as JobData).kind);
        batchNo++;
      } catch (err) {
        console.error('producer error (batch skipped):', err);
      }
      await sleep(100);
    }
  })();

  // ── Progress log ────────────────────────────────────────────────────────────
  const t0 = Date.now();
  const progress = setInterval(async () => {
    const [completed, failed, ready, active, delayed] = await Promise.all([
      redis.zcard(`${KP}:completed`), redis.zcard(`${KP}:failed`),
      redis.llen(`${KP}:ready`), redis.llen(`${KP}:active`), redis.zcard(`${KP}:delayed`),
    ]);
    console.log(
      `  [${String(Math.round((Date.now() - t0) / 1000)).padStart(3)}s] ` +
      `added=${addedKind.size} completed=${completed} failed=${failed} ` +
      `ready=${ready} active=${active} delayed=${delayed} kills=${kills}`,
    );
  }, 15000);

  console.log(`Soak: ${NUM_WORKERS} workers, SIGKILL every 2–4s, ${DURATION_MS / 1000}s of load\n`);
  await sleep(DURATION_MS);

  // ── Shutdown load, keep workers (and the killer off) for the drain ─────────
  producing = false;
  await producerLoop;
  killing = false;
  await killerLoop;
  // Make sure every slot has a live worker for the drain phase.
  for (let i = 0; i < NUM_WORKERS; i++) {
    if (!workers[i].alive) { spawnWorker(i); respawns++; }
  }
  console.log(`\nLoad stopped: ${addedKind.size} jobs added, ${kills} kills, ${respawns} respawns. Draining...`);

  // ── Drain: wait until every added job reached a terminal state ─────────────
  const DRAIN_TIMEOUT = 180_000;
  const drainStart = Date.now();
  let drained = false;
  while (Date.now() - drainStart < DRAIN_TIMEOUT) {
    const [completed, failed, ready, active, delayed, priority] = await Promise.all([
      redis.zcard(`${KP}:completed`), redis.zcard(`${KP}:failed`),
      redis.llen(`${KP}:ready`), redis.llen(`${KP}:active`),
      redis.zcard(`${KP}:delayed`), redis.zcard(`${KP}:priority`),
    ]);
    let groupWaiting = 0;
    for (let g = 0; g < GROUPS; g++) {
      groupWaiting += await redis.llen(`${KP}:group:g${g}`);
      groupWaiting += await redis.zcard(`${KP}:group:priority:g${g}`);
    }
    if (completed + failed >= addedKind.size &&
        ready + active + delayed + priority + groupWaiting === 0) {
      drained = true;
      break;
    }
    await sleep(2000);
  }
  console.log(drained
    ? `Drained in ${Math.round((Date.now() - drainStart) / 1000)}s.`
    : `DRAIN TIMEOUT after ${DRAIN_TIMEOUT / 1000}s — some jobs are stuck!`);

  // ── Accounting (workers still idle-running; queue is quiescent) ─────────────
  const ids = [...addedKind.keys()];
  let completedN = 0, failedN = 0, pendingN = 0;
  const lost: string[] = [];
  const inBoth: string[] = [];

  for (let off = 0; off < ids.length; off += 2000) {
    const chunk = ids.slice(off, off + 2000);
    const pipe = redis.pipeline();
    for (const id of chunk) {
      pipe.zscore(`${KP}:completed`, id);
      pipe.zscore(`${KP}:failed`, id);
    }
    const res = await pipe.exec();
    for (let i = 0; i < chunk.length; i++) {
      const inCompleted = res![i * 2][1] !== null;
      const inFailed = res![i * 2 + 1][1] !== null;
      if (inCompleted && inFailed) inBoth.push(chunk[i]);
      if (inCompleted) completedN++;
      else if (inFailed) failedN++;
      else {
        // Terminal nowhere — pending or lost. Distinguish via any live structure.
        const [inReady, inActive, inDelayed] = await Promise.all([
          redis.lpos(`${KP}:ready`, chunk[i]),
          redis.lpos(`${KP}:active`, chunk[i]),
          redis.zscore(`${KP}:delayed`, chunk[i]),
        ]);
        let inGroup = false;
        for (let g = 0; g < GROUPS && !inGroup; g++) {
          inGroup = (await redis.lpos(`${KP}:group:g${g}`, chunk[i])) !== null
            || (await redis.zscore(`${KP}:group:priority:g${g}`, chunk[i])) !== null;
        }
        if (inReady !== null || inActive !== null || inDelayed !== null || inGroup) pendingN++;
        else lost.push(chunk[i]);
      }
    }
  }

  // ── Invariants: counters, job-map, failed breakdown, duplicate runs ────────
  const runningBad: string[] = [];
  for (let g = 0; g < GROUPS; g++) {
    const v = await redis.get(`${KP}:running:g${g}`);
    if (v !== null && v !== '0') runningBad.push(`g${g}=${v}`);
  }
  const jobMapLen = await redis.hlen(`${KP}:group:job-map`);

  const failedIds = await redis.zrange(`${KP}:failed`, 0, -1);
  let plannedFails = 0, stalledFails = 0, otherFails = 0;
  for (const id of failedIds) {
    const reason = await redis.hget(`${KP}:job:${id}`, 'failedReason');
    if (reason === 'hard failure (planned)') plannedFails++;
    else if (reason?.includes('stalled')) stalledFails++;
    else otherFails++;
  }

  const execs = await redis.hgetall(EXEC_KEY);
  let extraRuns = 0;
  for (const [id, nStr] of Object.entries(execs)) {
    const kind = addedKind.get(id);
    const expected = kind === 'fail' ? 2 : 1;
    if (Number(nStr) > expected) extraRuns += Number(nStr) - expected;
  }

  // ── Report ──────────────────────────────────────────────────────────────────
  const check = (ok: boolean, label: string) => {
    console.log(`  ${ok ? '✓' : '✗'} ${label}`);
    return ok;
  };
  console.log('\n══════════════════ SOAK RESULT ══════════════════');
  console.log(`  Added            : ${addedKind.size}`);
  console.log(`  Completed        : ${completedN}`);
  console.log(`  Failed           : ${failedN}  (planned=${plannedFails}, stalled=${stalledFails}, other=${otherFails})`);
  console.log(`  Pending          : ${pendingN}`);
  console.log(`  Kills / respawns : ${kills} / ${respawns}`);
  console.log(`  Extra re-runs    : ${extraRuns}  (at-least-once re-runs from kills — informational)`);
  console.log('──────────────────────────────────────────────────');
  let ok = true;
  ok = check(lost.length === 0, `No lost jobs (lost=${lost.length}${lost.length ? ': ' + lost.slice(0, 5).join(', ') : ''})`) && ok;
  ok = check(drained, 'Queue fully drained (no group stalled)') && ok;
  ok = check(pendingN === 0, `Nothing left pending after drain (pending=${pendingN})`) && ok;
  ok = check(runningBad.length === 0, `All running:{group} counters at 0 (${runningBad.join(', ') || 'all zero'})`) && ok;
  ok = check(jobMapLen === 0, `group:job-map empty (len=${jobMapLen})`) && ok;
  ok = check(inBoth.length === 0, `No job both completed AND failed (${inBoth.length})`) && ok;
  ok = check(completedN + failedN === addedKind.size, 'completed + failed === added') && ok;
  console.log('══════════════════════════════════════════════════');
  console.log(ok ? '  SOAK PASSED ✓' : '  SOAK FAILED ✗');

  // ── Cleanup ─────────────────────────────────────────────────────────────────
  clearInterval(progress);
  killEverything();
  await queue.close();
  await redis.quit();
  console.log(`  (worker stderr log: ${workerLogPath})`);
  process.exit(ok ? 0 : 1);
}
