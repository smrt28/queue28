local logtable = {}
local q_list = KEYS[1]
local q_set = KEYS[2]
local q_count = KEYS[3]

local command = ARGV[1]


-- redis.call('del', '_debug')

local function log(msg)
    redis.call('rpush', '_debug', command .. " => " .. msg)
end

log("------------------------------------")



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
            if t['counter'] > 0 then
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
    local len = redis.call('llen', key)
    decrement_by(len)
    redis.call('del', key)
    redis.call('srem', q_set, key)
elseif command == 'put_counter' then

end

--[[
if false then
    setup_counter_queue("u1", "http://asobu/u1", 5)
    setup_counter_queue("u2", "http://asobu/u2", 4)
    setup_counter_queue("u3", "http://asobu/u3", 8)
    push("y4", "data1")
    push("y4", "data2")
    push("y4", "data3")
else
    local key, t, data
    key, t, data = pop()
    return {key, t, data}
end
]]--

--return pop()
-- return load_queue_info('smrt')
--local msg = "Hello, world!"

--local x = cjson.decode(a)

--return type(x)

--[[

]]--

