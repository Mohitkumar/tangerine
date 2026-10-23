-- Releases a lease only if the caller still holds it. Returns 1 if released.
-- KEYS[1] = {..}:lease   ARGV[1] = worker id
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
