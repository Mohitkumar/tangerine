-- Atomically commits one processed batch for one partition.
--
-- KEYS[1] = {..}:meta   KEYS[2] = {..}:state   KEYS[3] = {..}:out   (same hash slot)
--
-- ARGV layout:
--   1 epoch            the caller's lease epoch; must equal meta.epoch
--   2 lastInputId      new input offset
--   3 watermark
--   4 producerId       stamped on every output entry
--   then: nPuts,  (field, value) * nPuts      state writes
--   then: nDels,  field * nDels               state deletes
--   then: nOut,   (dst, ts, payload) * nOut   outbox appends
--   then: nSeen,  (producer, seq) * nSeen     highest seq seen per upstream producer
--
-- Returns the new out_seq. Errors with FENCED if the epoch does not match, in which case
-- nothing has been written.
local meta, state, out = KEYS[1], KEYS[2], KEYS[3]

local current = redis.call('HGET', meta, 'epoch')
if current ~= ARGV[1] then
  return redis.error_reply('FENCED partition epoch is ' .. tostring(current) .. ', caller has ' .. ARGV[1])
end

local producer = ARGV[4]
local i = 5

local nPuts = tonumber(ARGV[i]); i = i + 1
for _ = 1, nPuts do
  redis.call('HSET', state, ARGV[i], ARGV[i + 1])
  i = i + 2
end

local nDels = tonumber(ARGV[i]); i = i + 1
for _ = 1, nDels do
  redis.call('HDEL', state, ARGV[i])
  i = i + 1
end

local seq = tonumber(redis.call('HGET', meta, 'out_seq') or '0')
local nOut = tonumber(ARGV[i]); i = i + 1
for _ = 1, nOut do
  seq = seq + 1
  redis.call('XADD', out, '*', 'dst', ARGV[i], 'ts', ARGV[i + 1], 'p', producer, 's', seq, 'v', ARGV[i + 2])
  i = i + 3
end

local nSeen = tonumber(ARGV[i]); i = i + 1
for _ = 1, nSeen do
  redis.call('HSET', meta, 'seen:' .. ARGV[i], ARGV[i + 1])
  i = i + 2
end

redis.call('HSET', meta, 'in_offset', ARGV[2], 'wm', ARGV[3], 'out_seq', seq)
return seq
