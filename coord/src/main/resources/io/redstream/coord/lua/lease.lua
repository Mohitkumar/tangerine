-- Acquires or renews a partition lease and returns the epoch the holder must write with.
--
-- KEYS[1] = {..}:lease   KEYS[2] = {..}:meta   (same hash slot)
-- ARGV[1] = worker id    ARGV[2] = ttl in ms
--
-- Free lease: take it and bump the epoch, which fences any previous owner's writes.
-- Held by caller: extend the TTL and return the current epoch.
-- Held by someone else: return -1.
local holder = redis.call('GET', KEYS[1])
if holder == false then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return redis.call('HINCRBY', KEYS[2], 'epoch', 1)
elseif holder == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return tonumber(redis.call('HGET', KEYS[2], 'epoch'))
end
return -1
