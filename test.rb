require_relative './q28'
require 'redis'

redis = Redis.new
redis.call 'FLUSHALL'
q = Queue28.new(redis, 'smrt', debug: true)
q.update_udf
q.clear_log

puts q.put_counter 'cmrt', 'xsomedatax', 10

q.push 'smrt1', 'somedata1'
q.push 'smrt1', 'somedata2'
q.push 'smrt1', 'somedata3'
q.push 'smrt1', 'somedata4'
q.push 'smrt1', 'somedata5'
q.push 'smrt1', 'somedata6'
q.push 'smrt2', 'somedata1x'
q.push 'smrt2', 'somedata2x'
q.push 'smrt2', 'somedata3x'
q.push 'smrt2', 'somedata4x'
q.push 'smrt2', 'somedata5x'
q.push 'smrt2', 'somedata6x'
q.push 'smrt3', 'somedata1y'
q.push 'smrt3', 'somedata2y'
q.push 'smrt3', 'somedata3y'
q.push 'smrt3', 'somedata4y'
q.push 'smrt3', 'somedata5y'
q.push 'smrt3', 'somedata6y'


puts q.len

cnt = 0
loop do
    cnt += 1; break if cnt == 1000

    if cnt == 10
        q.put_counter 'cmrt2', 'xsomedatax', 3
    end
    res = q.pop
    break if res.nil?
    puts res.to_s
    puts q.len_of 'smrt2'
end

puts q.len
puts '--'
puts q.ping
puts q.sha
