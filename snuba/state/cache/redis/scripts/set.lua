-- KEYS[1]: The value key.
-- KEYS[2]: The waiting queue key.
-- KEYS[3]: The task unique ID key.
-- KEYS[4]: The notify queue key.
-- ARGV[1]: The task unique ID.
-- ARGV[2]: The notify queue TTL.
-- ARGV[3]: The value. (optional)
-- ARGV[4]: The value TTL. (optional)

-- Check to make sure that the current task is still the task that we are
-- responsible for executing. If it doesn't exist or does not match the current
-- task ID, we must have overrun the timeout.
-- TODO: This may still be able to safely set the cache value?
local task_id = redis.call('GET', KEYS[3])
if not task_id or task_id ~= ARGV[1] then
    return {err="invalid task id"}
end

-- Update the cache value.
if ARGV[3] ~= nil then
    redis.call('SETEX', KEYS[1], ARGV[4], ARGV[3])
end

-- Move the data from the waiting queue to the notify queue.
redis.call('RENAME', KEYS[2], KEYS[4])
redis.call('EXPIRE', KEYS[4], ARGV[2])

-- Remove one item (representing our own entry) from the notify queue.
redis.call('LPOP', KEYS[4])

-- Delete the task unique key.
redis.call('DEL', KEYS[3])
