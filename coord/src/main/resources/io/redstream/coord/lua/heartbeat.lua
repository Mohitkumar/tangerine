-- Records a worker heartbeat, evicts stale workers, and returns all live worker ids.
-- Uses Redis server time so worker clock skew cannot affect membership.
--
-- KEYS[1] = {job}:workers (ZSET)   ARGV[1] = worker id   ARGV[2] = ttl in ms
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
redis.call('ZADD', KEYS[1], now, ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - tonumber(ARGV[2]))
return redis.call('ZRANGE', KEYS[1], 0, -1)
