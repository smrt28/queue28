require 'redis'

class Queue28
    private
    def _call method, args = []
        @redis.evalsha @sha, @keys, [ method, @name ] + args
    end

    public
    def initialize redis, name
        @name = name
        @keys = [ "q_list_#{name}", "q_set_#{name}" ]
        @redis = redis
        udf = IO.read('udf/queues.lua')
        @sha = @redis.script :load, udf
    end



    attr_reader :sha

    def push instance, data
        _call 'push', [ instance, data ]
    end

    def pop
        _call 'pop'

       #@redis.evalsha @sha, @keys, [ 'pop', @name ]
    end
end


q = Queue28.new(Redis.new, 'smrt')


puts q.pop.to_s

q.push 'smrt1', 'somedata1'
q.push 'smrt1', 'somedata2'

puts q.pop.to_s
puts q.pop.to_s


=begin


q28_push = IO.read 'udf/q28-push.lua'
q28_pop = IO.read 'udf/q28-pop.lua'

r = Redis.new


push = r.script :load, q28_push
pop = r.script :load, q28_pop


# instance, data
puts r.evalsha push, ['q'], ["u2", Time.now.to_s]


#puts r.evalsha pop, ['q'], []

#r.script :load, q28_push

=end
