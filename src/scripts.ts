// Atomically checks group size and enqueues the job in one round-trip.
// maxSize < 0 means "unlimited" (no size check performed).
// Returns 1 on success, 0 when maxSize would be exceeded.
export const LUA_GROUP_ENQUEUE = `
local listKey  = KEYS[1]
local zsetKey  = KEYS[2]
local maxSize  = tonumber(ARGV[1])
local jobId    = ARGV[2]
local priority = tonumber(ARGV[3])
if maxSize >= 0 then
  local sz = redis.call('llen', listKey) + redis.call('zcard', zsetKey)
  if sz >= maxSize then return 0 end
end
if priority > 0 then
  redis.call('zadd', zsetKey, priority, jobId)
else
  redis.call('rpush', listKey, jobId)
end
return 1
`;

// Atomically records dispatch bookkeeping (running counter + ownership map)
// and pushes the job onto :ready in one round-trip, so a process crash
// between these steps can no longer leave the group counter incremented
// with no job-map entry to recover it.
export const LUA_DISPATCH_JOB = `
local runningKey = KEYS[1]
local jobMapKey  = KEYS[2]
local readyKey   = KEYS[3]
local groupId    = ARGV[1]
local jobId      = ARGV[2]
redis.call('incr', runningKey)
redis.call('hset', jobMapKey, jobId, groupId)
redis.call('rpush', readyKey, jobId)
return 1
`;
