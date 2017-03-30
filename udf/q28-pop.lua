
local prefix = KEYS[1]

local qset = prefix .. '-set'
local qlist = prefix .. '-list'
local qqueue = prefix .. '-queue'
local qchan = prefix .. '-channel'


selected_queue = redis.call('lpop', qlist)
if selected_queue == nil then
    return nil
end

rv = redis.call('lpop', qqueue)

if rv == nil then
    redis.call('zrem', qset, ...)
end

redis.call('lpushx', qlist, selected_queue)



