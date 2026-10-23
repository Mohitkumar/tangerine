-- Records forwarding progress and trims delivered outbox entries.
--
-- KEYS[1] = {..}:meta   KEYS[2] = {..}:out
-- ARGV[1] = epoch       ARGV[2] = id of the last forwarded entry
--
-- Fenced like commit.lua, so a zombie forwarder cannot move the offset or trim entries that
-- the new owner has not forwarded yet. MINID keeps the acked entry itself; reads start
-- strictly after fwd_offset, so it is never forwarded twice by the same owner.
local current = redis.call('HGET', KEYS[1], 'epoch')
if current ~= ARGV[1] then
  return redis.error_reply('FENCED partition epoch is ' .. tostring(current) .. ', caller has ' .. ARGV[1])
end
redis.call('HSET', KEYS[1], 'fwd_offset', ARGV[2])
return redis.call('XTRIM', KEYS[2], 'MINID', ARGV[2])
