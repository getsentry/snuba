-- KEYS[1]: The value key.
-- KEYS[2]: The waiting queue key.
-- KEYS[3]: The task unique ID key.
-- ARGV[1]: The task execution timeout. Only used when creating a new task.
-- ARGV[2]: The task unique ID. Only used when creating a new task.

-- Check to see if a value already exists at the result key. If one does, we
-- don't have to do anything other than return it and exit.
local value = redis.call('GET', KEYS[1])
if value then
    return {0, value}
end

-- Check to see if a waiting queue has already been established. If we are the
-- only member of the queue, we can proceed with the task. Otherwise, we need to
-- wait to be notified of task completion, or for the timeout to be reached,
-- whichever comes first.
local waiting = redis.call('RPUSH', KEYS[2], '')
if waiting == 1 then
    redis.call('EXPIRE', KEYS[2], ARGV[1])
    -- We shouldn't be overwriting an existing task here, but it's safe if we
    -- do, given that the queue was empty.
    redis.call('SETEX', KEYS[3], ARGV[1], ARGV[2])
    return {1, ARGV[2], ARGV[1]}
else
    return {2, redis.call('GET', KEYS[3]), redis.call('TTL', KEYS[3])}
end
