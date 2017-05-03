-- LUA implementation of redis round-robin QUEUES
--
-- The script maintains a set of queues. There are 2 types od queues:
--
-- Redis LIST - this is standard FIFO queue.
--
-- Redis HASH - the queue if sequence of numbers (1..N). The implementation
-- is realized by counter which decrements by every pop.
--
--
-- The script maintains:
--      redis list q_list - the list of redis keys of queues
--      redis set q_set - the set of non-empty queues (redis keys)
--      redis number q_count - the total number of all items in all queues (even couner queues)
--
--
-- POP
--
--  You have to select a queue first. The queue is selected by
--  select_queue() function. select_queue reads the first element from
--  q_list (list of queues). The read element is a redis key of queue.
--  If the selected queue is empty, it removes the queue also from
--  q_set and continues reading from q_list until an non-empty queue is found.
--  If an non empty queue is found then it is pushed back to q_list and
--  the name of the queue is returned.
--
--  You can pop from the selected queue by an usual way. LPOP form FIFO
--  queue or DECR from couner queue.
--
--  It is possible that an another process would pop the last item from the
--  queue which you have selected. In this case pop would return null
--  and you'll have to select another queue.
--
-- PUSH
--
--  It simly pushes the item to the queue. Then if the queue is already
--  in q_set it's finished. Otherwise, it appens the queue to q_list and
--  to q_set. (this is why we need q_set - to recognize if q_list already
--  includes the queue)
--
--
--


local logtable = {}
local q_list = KEYS[1]
local q_set = KEYS[2]
local q_count = KEYS[3]

local command = ARGV[1]

local function log(msg)
    redis.call('rpush', '_debug', command .. " => " .. msg)
end

log("------------------------------------")

local function decrement()
    redis.call('decr', q_count)
end

local function decrement_by(i)
    redis.call('decrby', q_count, i)
end

local function increment()
    redis.call('incr', q_count)
end

local function increment_by(i)
    redis.call('incrby', q_count, i)
end


local function store_queue_info(key, m)
    for k,v in pairs(m) do
        redis.call ('hset', key, k, v)
    end
end

local function load_queue_info(key)
    local rv = {}
    local tpe = redis.call('type', key)['ok']
    if tpe == 'list' then
        return { t = 'ordinary' }
    end

    local t = redis.call('hgetall', key)
    local k = nil; local v = nil

    for _, i in pairs(t) do
        if k == nil then
            k = i
        else
            rv[k] = i
            k = nil
        end
    end
    return rv
end

local function schedule(key)
    log("cheduling: " .. key)
    local cnt = redis.call('sismember', q_set, key)
    if cnt > 0 then
        log("already scheduled: " .. key)
        return
    end
    redis.call('sadd', q_set, key)
    redis.call('lpush', q_list, key)
end

local function setup_counter_queue(key, data, count)
    if count <= 0 then return 'ok' end
    local tpe = redis.call('type', key)['ok']
    if not (tpe == 'hash' or tpe == 'none') then
        return 'exists'
    end

    if tpe == 'hash' then
        info = load_queue_info(key)
        c = tonumber(info['counter'])
        decrement_by(c)
    end

    local o = {
        t = 'counter',
        data = data,
        counter = count
    }

    store_queue_info(key, o)

    if count > 0 then
        increment_by(count)
        schedule(key)
    end

    return 'ok'
end


local function push(instance, data)
    log("push to: " .. instance .. "; data=" .. data)
    increment()
    redis.call ('lpush', instance, data)
    return schedule(instance)
end


local function pop(key)
    log("pop from: " .. key)
    local t = load_queue_info(key)
    if t['t'] == 'counter' then
        local c = tonumber(t['counter'])
        if c > 0 then
            t['counter'] = c - 1
            store_queue_info(key, t)
            if c <= 1 then
                redis.call('srem', q_set, key)
            end
            decrement()
            return key, 'counter', t['data']
        end
    elseif t['t'] == 'ordinary' then
        local data = redis.call('rpop', key)
        if data then
            local len = redis.call('llen', key)
            log("got data from: " .. key .. " len=" .. tostring(len))
            if len == 0 then
                redis.call('srem', q_set, key)
            end

            decrement()
            return key, 'ordinary', data
        else
            redis.call('srem', q_set, key)
        end
    end
end


local function select_queue()
    while true do
        local key = redis.call('rpop', q_list)
        if not key then return nil end
        local t = load_queue_info(key)
        if t['t'] == 'counter' then
            if tonumber(t['counter']) > 0 then
                redis.call('lpush', q_list, key)
                log("selecting queue: " .. key)
                return key
            end
            redis.call('srem', q_set, key)
        elseif t['t'] == 'ordinary' then
            local len = redis.call('llen', key)
            log("len of " .. key .. " id " .. tostring(len))
            if len > 0 then
                redis.call('lpush', q_list, key)
                log("selecting queue: " .. key)
                return key
            end
            redis.call('srem', q_set, key)
        end

        log("failed to select queue: " .. key )
    end
end

local function clear(key)
    local t = load_queue_info(key)
    if t['t'] == 'counter' then
        local c = tonumber(t['counter'])
        decrement_by(c)
        redis.call('del', key)
        redis.call('srem', q_set, key)
        return c
    end

    local len = redis.call('llen', key)
    decrement_by(len)
    redis.call('del', key)
    redis.call('srem', q_set, key)
    return len
end

local function len_of(key)
    local t = load_queue_info(key)
    if t['t'] == 'counter' then
        return tonumber(t['counter'])
    end

    return redis.call('llen', key)
end


if command == "select_queue" then
    return select_queue()
elseif command == "push" then
    return push(KEYS[4], ARGV[2])
elseif command == "pop" then
    local key, t, data
    key, t, data = pop(KEYS[4])
    if key == nil then return nil end
    return {key, t, data}
elseif command == "clear" then
    local key = KEYS[4]
    clear(key)
elseif command == 'put_counter' then
    local count = tonumber(ARGV[3])
    local data = ARGV[2]
    setup_counter_queue(KEYS[4], data, count)
elseif command == 'len_of' then
    return len_of(KEYS[1])
elseif command == 'ping' then
    return 'pong'
end
