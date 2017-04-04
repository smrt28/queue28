local logtable = {}
local q_list = KEYS[1]
local q_set = KEYS[2]
local command = ARGV[1]
local name = ARGV[2]


-- redis.call('del', '_debug')

local function log(msg)
    -- redis.call('rpush', '_debug', command .. " => " .. msg)
end

log("***")



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

local function setup_weak_queue(key, uri, count)
    local o = {
        t = 'weak',
        uri = uri,
        counter = count
    }

    store_queue_info(key, o)

    if count > 0 then
        schedule(key)
    end
end


local function push(instance, data)
    redis.call ('lpush', instance, data)
    return schedule(instance)
end


local function pop()
    local cnt = 0
    while true do
        cnt = cnt + 1
        local key = redis.call('rpop', q_list)

        if not key then
            return nil
        end
        local t = load_queue_info(key)
        if t['t'] == 'weak' then
            local c = tonumber(t['counter'])
            if c > 0 then
                t['counter'] = c - 1
                store_queue_info(key, t)
                if c > 1 then
                    redis.call('lpush', q_list, key)
                else
                    redis.call('srem', q_set, key)
                end
                return key, 'weak', t['uri']
            end
        elseif t['t'] == 'ordinary' then
            local data = redis.call('rpop', key)
            if data then
                redis.call('lpush', q_list, key)
                local len = redis.call('llen', key)
                log("got data from: " .. key ..
                    " len=" .. tostring(len))
                if len == 0 then
                    redis.call('srem', q_set, key)
                end

                return key, 'ordinary', data
            else
                redis.call('srem', q_set, key)
            end
        end
    end
end

if command == "push" then
    local instance = ARGV[3]
    local data = ARGV[4]
    return push(name .. "/" .. instance, data)
elseif command == "pop" then
    local key, t, data
    key, t, data = pop()
    return {key, t, data}
end

--[[
if false then
    setup_weak_queue("u1", "http://asobu/u1", 5)
    setup_weak_queue("u2", "http://asobu/u2", 4)
    setup_weak_queue("u3", "http://asobu/u3", 8)
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

