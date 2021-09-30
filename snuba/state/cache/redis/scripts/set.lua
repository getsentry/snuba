-- KEYS[1]: The value key.
-- KEYS[2]: The waiting queue key.
-- KEYS[3]: The task unique ID key.
-- KEYS[4]: The notify queue key.
local value_key = KEYS[1]
local wait_queue_key = KEYS[2]
local task_id_key = KEYS[3]
local notify_queue_key = KEYS[4]

-- ARGV[1]: The task unique ID.
local task_id = ARGV[1]
local notify_queue_ttl_s = ARGV[2]
local value = ARGV[3]
local value_ttl_s = ARGV[4]
-- ARGV[2]: The notify queue TTL.
-- ARGV[3]: The value. (optional)
-- ARGV[4]: The value TTL. (optional)

-- Check to make sure that the current task is still the task that we are
-- responsible for executing. If it doesn't exist or does not match the current
-- task ID, we must have overrun the timeout.
-- TODO: This may still be able to safely set the cache value?
local task_id = redis.call('GET', task_id_key)
if not task_id or task_id ~= task_id then
    return {err="invalid task id"}
end

-- Update the cache value.
if value ~= nil then
    redis.call('SET', value_key,  value, "PX", math.floor(value_ttl_s * 1000))
end

-- Move the data from the waiting queue to the notify queue.
redis.call('RENAME', wait_queue_key, notify_queue_key)
redis.call('PEXPIRE', notify_queue_key, math.floor(notify_queue_ttl_s * 1000))

-- Remove one item (representing our own entry) from the notify queue.
redis.call('LPOP', notify_queue_key)

-- Delete the task unique key.
redis.call('DEL', task_id_key)
