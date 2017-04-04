--[[
local logtable = {}

local function log(msg)
      logtable[#logtable+1] = msg
end
]]--

local q_set = 'q_set'
local q_list = 'q_list'

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
    local cnt = redis.call('sismember', q_set, key)
    if cnt > 0 then return end
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
    schedule(instance)
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
                return key, 'ordinary', data
            else
                redis.call('srem', q_set, key)
            end
        end
    end
end



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
--return pop()
-- return load_queue_info('smrt')
--local msg = "Hello, world!"

--local x = cjson.decode(a)

--return type(x)

--[[

]]--

