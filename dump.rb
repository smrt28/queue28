require 'redis'

redis = Redis.new

S = 'q_set_smrt'
L = 'q_list_smrt'

active = redis.sscan(S, 0)

puts "active: #{active[1]}"

puts "------------------------------------"

#a = redis.sscan 'q_set', 0
a = redis.lrange L, 0, 1000
cnt = 0
a.each do |key|
    type = redis.type key
    case type
    when 'hash'
        h = redis.hgetall(key)
        puts "#{cnt}) #{key}: #{h}"
    when 'list'
        len = redis.llen key
        puts "#{cnt}) #{key}: len=#{len}"

    end
    cnt += 1
end


puts "------------------------------------"

a = redis.lrange '_debug', 0, 1000
a.each do |msg|
    puts "log: #{msg}"
end

