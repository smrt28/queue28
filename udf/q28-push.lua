
local instance = ARGV[1]
local data = ARGV[2]
local prefix = KEYS[1]

local qset = prefix .. '-set'
local qlist = prefix .. '-list'
local qqueue = prefix .. '-queue'
local qchan = prefix .. '-channel'

local cnt = redis.call('zcount', qset, -1, 1)

redis.call('zadd', qset, 1, instance)

if cnt == 0 then
   redis.call('lpush', qlist, instance)
end

redis.call('lpush', qqueue, data)

redis.call('publish', qchan, "message")

return cnt

