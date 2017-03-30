require 'redis'

module Queue28
class List
    def initialize redis
        @redis = redis
    end

end
end





q28_push = IO.read 'udf/q28-push.lua'
q28_pop = IO.read 'udf/q28-pop.lua'

r = Redis.new


push = r.script :load, q28_push
pop = r.script :load, q28_pop


# instance, data
#puts r.evalsha push, ['q'], ["u2", Time.now.to_s]


puts r.evalsha pop, ['q'], []

#r.script :load, q28_push


