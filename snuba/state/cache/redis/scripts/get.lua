-- KEYS[1]: The value key.
-- KEYS[2]: The waiting queue key.
-- KEYS[3]: The task unique ID key.
-- ARGV[1]: The task execution timeout. Only used when creating a new task.
-- ARGV[2]: The task unique ID. Only used when creating a new task.
local value_key = KEYS[1]
local wait_queue_key = KEYS[2]
local task_id_key = KEYS[3]
local error_key = KEYS[4]
local task_timeout_s = ARGV[1]
local task_id = ARGV[2]

local CODE_RESULT_VALUE = 0
local CODE_RESULT_EXECUTE = 1
local CODE_RESULT_WAIT = 2
local CODE_RESULT_ERROR = 3

-- Check to see if a value already exists at the result key. If one does, we
-- don't have to do anything other than return it and exit.
local value = redis.call('GET', value_key)
if value then
    return {CODE_RESULT_VALUE, value}
end

local err = redis.call('GET', error_key)
if err then
    return {CODE_RESULT_VALUE, err}
end


-- Check to see if a waiting queue has already been established. If we are the
-- only member of the queue, we can proceed with the task. Otherwise, we need to
-- wait to be notified of task completion, or for the timeout to be reached,
-- whichever comes first.
local waiting = redis.call('RPUSH', wait_queue_key, '')
if waiting == 1 then
    redis.call('PEXPIRE', wait_queue_key, math.floor(task_timeout_s * 1000))
    -- We shouldn't be overwriting an existing task here, but it's safe if we
    -- do, given that the queue was empty.
    redis.call('SET', task_id_key, task_id, "PX", math.floor(task_timeout_s * 1000))
    return {CODE_RESULT_EXECUTE, task_id, task_timeout_s}
else
    return {CODE_RESULT_WAIT, redis.call('GET', task_id_key), redis.call('TTL', task_id_key)}
end
