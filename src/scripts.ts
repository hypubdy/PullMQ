import type Redis from 'ioredis';

// Atomically checks group size, enqueues the job AND registers the group in the
// round-robin rotation in one round-trip.
//
// Folding the SADD/RPUSH registration into this script closes the crash window
// where a job landed in the group list but the process died before the group
// was added to groups:set/groups:active — leaving the job stranded because
// every later SADD returns 0 and never re-pushes the group (ISSUES.md #6).
//
// maxSize < 0 means "unlimited" (no size check performed) — kept distinct from
// an explicit maxSize:0, which rejects every enqueue.
// ARGV[5] selects FIFO placement: 'rpush' (tail, normal add) or 'lpush' (front,
// rate-limited/stalled re-enqueue).
// Returns 1 on success, 0 when maxSize would be exceeded.
export const LUA_GROUP_ENQUEUE = `
local listKey   = KEYS[1]
local zsetKey   = KEYS[2]
local setKey    = KEYS[3]
local activeKey = KEYS[4]
local maxSize   = tonumber(ARGV[1])
local jobId     = ARGV[2]
local priority  = tonumber(ARGV[3])
local groupId   = ARGV[4]
local pushCmd   = ARGV[5]
if maxSize >= 0 then
  local sz = redis.call('llen', listKey) + redis.call('zcard', zsetKey)
  if sz >= maxSize then return 0 end
end
if priority > 0 then
  redis.call('zadd', zsetKey, priority, jobId)
else
  redis.call(pushCmd, listKey, jobId)
end
if redis.call('sadd', setKey, groupId) == 1 then
  redis.call('rpush', activeKey, groupId)
end
return 1
`;

// Atomically pops the next job from a group (priority zset first, then FIFO
// list), enforces the concurrency ceiling, records dispatch bookkeeping
// (running counter + ownership map) and pushes the job onto :ready — all in
// one script, so a crash can never strand a popped-but-not-dispatched job.
//
// The ceiling check lives INSIDE the script (before any pop) so the "never
// exceed maxGroupConcurrency" invariant is guaranteed by Redis atomicity
// instead of depending on the 30s scheduler lock never expiring mid-pass
// (ISSUES.md #7). max < 0 means "unlimited".
//
// HSETNX is the duplicate gate: if a popped jobId already has an in-flight
// dispatch (a stalled-recovery copy racing the still-running original), a
// second dispatch would INCR the counter twice while the map can only ever be
// HDEL'd once — leaking the slot forever. Duplicates are dropped and popping
// continues.
// Returns the dispatched job id, or nil when the group is empty or at capacity.
export const LUA_POP_DISPATCH = `
local zsetKey    = KEYS[1]
local listKey    = KEYS[2]
local runningKey = KEYS[3]
local jobMapKey  = KEYS[4]
local readyKey   = KEYS[5]
local groupId    = ARGV[1]
local max        = tonumber(ARGV[2])
if max >= 0 then
  local current = tonumber(redis.call('get', runningKey) or '0')
  if current >= max then return false end
end
while true do
  local jobId
  local popped = redis.call('zpopmin', zsetKey, 1)
  if #popped > 0 then jobId = popped[1] else jobId = redis.call('lpop', listKey) end
  if not jobId then return false end
  if redis.call('hsetnx', jobMapKey, jobId, groupId) == 1 then
    redis.call('incr', runningKey)
    redis.call('rpush', readyKey, jobId)
    return jobId
  end
end
`;

// Atomically finishes (or reclaims) a picked-up job: detaches it from :active
// and its processing lock, releases the group slot (HDEL-gated) and attaches
// it to its destination — completed/failed/delayed zset, :ready, or its group
// queue — in ONE script.
//
// Detach and attach used to be separate round-trips; a worker killed between
// them left the job in no structure at all: invisible to the stalled checker,
// lost forever (found by the kill-soak test). Any path that moves a job out of
// :active MUST go through this script.
//
// guard='1' is the stalled-checker variant: instead of a token-gated lock
// release it requires the lock to be ABSENT and the LREM to actually remove
// the job, so two checkers (or a checker racing the live worker) cannot both
// reclaim the same job.
// Returns 1 when moved, 0 when the guard rejected the reclaim.
export const LUA_FINISH_JOB = `
local activeKey    = KEYS[1]
local lockKey      = KEYS[2]
local jobMapKey    = KEYS[3]
local runningKey   = KEYS[4]
local destKey      = KEYS[5]
local setKey       = KEYS[6]
local gActiveKey   = KEYS[7]
local jobId        = ARGV[1]
local token        = ARGV[2]
local groupId      = ARGV[3]
local mode         = ARGV[4]
local score        = ARGV[5]
local pushCmd      = ARGV[6]
local register     = ARGV[7]
local guard        = ARGV[8]
if guard == '1' then
  if redis.call('exists', lockKey) == 1 then return 0 end
  if redis.call('lrem', activeKey, 1, jobId) == 0 then return 0 end
else
  if redis.call('get', lockKey) == token then redis.call('del', lockKey) end
  redis.call('lrem', activeKey, 1, jobId)
end
if groupId ~= '' then
  if redis.call('hdel', jobMapKey, jobId) == 1 then
    local r = redis.call('decr', runningKey)
    if r < 0 then redis.call('set', runningKey, '0') end
  end
end
if mode == 'zadd' then
  redis.call('zadd', destKey, score, jobId)
elseif mode == 'list' then
  redis.call(pushCmd, destKey, jobId)
end
if register == '1' and groupId ~= '' then
  if redis.call('sadd', setKey, groupId) == 1 then
    redis.call('rpush', gActiveKey, groupId)
  end
end
return 1
`;

// Atomically claims a due delayed job (ZREM is the claim gate) and routes it —
// group queue (with maxSize enforcement and defer-on-full), priority zset, or
// :ready — in one script. The old claim-then-enqueue two-step lost the job if
// the process died in between (found by the kill-soak test).
// Returns 1 promoted, 0 lost the claim race, 2 deferred (group at maxSize).
export const LUA_PROMOTE_JOB = `
local delayedKey  = KEYS[1]
local groupList   = KEYS[2]
local groupZset   = KEYS[3]
local setKey      = KEYS[4]
local gActiveKey  = KEYS[5]
local priorityKey = KEYS[6]
local readyKey    = KEYS[7]
local jobId       = ARGV[1]
local groupId     = ARGV[2]
local gPriority   = tonumber(ARGV[3])
local jPriority   = tonumber(ARGV[4])
local maxSize     = tonumber(ARGV[5])
local deferScore  = ARGV[6]
if redis.call('zrem', delayedKey, jobId) == 0 then return 0 end
if groupId ~= '' then
  if maxSize >= 0 then
    local sz = redis.call('llen', groupList) + redis.call('zcard', groupZset)
    if sz >= maxSize then
      redis.call('zadd', delayedKey, deferScore, jobId)
      return 2
    end
  end
  if gPriority > 0 then
    redis.call('zadd', groupZset, gPriority, jobId)
  else
    redis.call('rpush', groupList, jobId)
  end
  if redis.call('sadd', setKey, groupId) == 1 then
    redis.call('rpush', gActiveKey, groupId)
  end
elseif jPriority > 0 then
  redis.call('zadd', priorityKey, jPriority, jobId)
else
  redis.call('rpush', readyKey, jobId)
end
return 1
`;

// Atomically retires a drained group from the round-robin rotation: only when
// BOTH the FIFO list and the priority zset are empty does it remove the group
// from groups:active (all occurrences, purging any duplicates) and groups:set.
//
// Doing the emptiness check and the removal in one script closes the race with
// a concurrent enqueue: either the enqueue lands first (group is non-empty, we
// keep it) or the retire lands first (the enqueue's SADD then returns 1 and
// re-registers the group). No interleaving can strand a job (ISSUES.md #5).
// Returns 1 when retired, 0 when the group still has waiting jobs.
export const LUA_GROUP_RETIRE = `
local listKey   = KEYS[1]
local zsetKey   = KEYS[2]
local setKey    = KEYS[3]
local activeKey = KEYS[4]
local groupId   = ARGV[1]
if redis.call('llen', listKey) == 0 and redis.call('zcard', zsetKey) == 0 then
  redis.call('lrem', activeKey, 0, groupId)
  redis.call('srem', setKey, groupId)
  return 1
end
return 0
`;

// Atomically pops the highest-priority standalone job and moves it into
// :active, so a crash between "pop" and "record as active" cannot make the job
// vanish from every structure the stalled checker scans (ISSUES.md #4 — this
// is the priority-zset counterpart of the LMOVE used for :ready).
// Returns the job id, or nil when the zset is empty.
export const LUA_PICKUP_PRIORITY = `
local priorityKey = KEYS[1]
local activeKey   = KEYS[2]
local popped = redis.call('zpopmin', priorityKey, 1)
if #popped == 0 then return false end
redis.call('rpush', activeKey, popped[1])
return popped[1]
`;

// Writes the job hash ONLY if it still exists. Worker-side saves (activation,
// completion, failure) go through this so that a job removed mid-flight by
// Queue.remove()/Job.remove() is not silently resurrected by a later HSET
// (ISSUES.md P3). ARGV is a flat field/value list.
// Returns 1 when written, 0 when the hash is gone.
export const LUA_SAVE_JOB_IF_EXISTS = `
if redis.call('exists', KEYS[1]) == 0 then return 0 end
redis.call('hset', KEYS[1], unpack(ARGV))
return 1
`;

// ─── defineCommand plumbing ───────────────────────────────────────────────────
// Scripts are registered once per connection via ioredis defineCommand, so each
// call transmits only the script's SHA instead of the full source (EVALSHA
// under the hood) — see ISSUES.md P1.

export interface PullMQScripts {
  pmqGroupEnqueue(
    listKey: string, zsetKey: string, groupsSetKey: string, groupsActiveKey: string,
    maxSize: string, jobId: string, priority: string, groupId: string, pushCmd: 'lpush' | 'rpush',
  ): Promise<number>;
  pmqPopDispatch(
    zsetKey: string, listKey: string, runningKey: string, jobMapKey: string, readyKey: string,
    groupId: string, maxConcurrency: string,
  ): Promise<string | null>;
  pmqFinishJob(
    activeKey: string, lockKey: string, jobMapKey: string, runningKey: string,
    destKey: string, groupsSetKey: string, groupsActiveKey: string,
    jobId: string, token: string, groupId: string,
    mode: 'zadd' | 'list' | 'none', score: string, pushCmd: 'lpush' | 'rpush',
    register: '0' | '1', guard: '0' | '1',
  ): Promise<number>;
  pmqPromoteJob(
    delayedKey: string, groupListKey: string, groupZsetKey: string,
    groupsSetKey: string, groupsActiveKey: string, priorityKey: string, readyKey: string,
    jobId: string, groupId: string, groupPriority: string, jobPriority: string,
    maxSize: string, deferScore: string,
  ): Promise<number>;
  pmqRetireGroup(
    listKey: string, zsetKey: string, groupsSetKey: string, groupsActiveKey: string,
    groupId: string,
  ): Promise<number>;
  pmqPickupPriority(priorityKey: string, activeKey: string): Promise<string | null>;
  pmqSaveJobIfExists(jobKey: string, ...fieldValues: string[]): Promise<number>;
}

export type ScriptedRedis = Redis & PullMQScripts;

/**
 * Returns the client with PullMQ's Lua commands registered. Registration is
 * lazy and idempotent, so this is safe to call on every use — including on
 * user-provided Redis instances that never went through createClient().
 */
export function scripted(client: Redis): ScriptedRedis {
  const c = client as ScriptedRedis;
  if (typeof c.pmqGroupEnqueue !== 'function') {
    client.defineCommand('pmqGroupEnqueue', { numberOfKeys: 4, lua: LUA_GROUP_ENQUEUE });
    client.defineCommand('pmqPopDispatch', { numberOfKeys: 5, lua: LUA_POP_DISPATCH });
    client.defineCommand('pmqFinishJob', { numberOfKeys: 7, lua: LUA_FINISH_JOB });
    client.defineCommand('pmqPromoteJob', { numberOfKeys: 7, lua: LUA_PROMOTE_JOB });
    client.defineCommand('pmqRetireGroup', { numberOfKeys: 4, lua: LUA_GROUP_RETIRE });
    client.defineCommand('pmqPickupPriority', { numberOfKeys: 2, lua: LUA_PICKUP_PRIORITY });
    client.defineCommand('pmqSaveJobIfExists', { numberOfKeys: 1, lua: LUA_SAVE_JOB_IF_EXISTS });
  }
  return c;
}
