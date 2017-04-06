require 'redis'

class Queue28
    private
    def _call method, args = [],
            instance: nil

        keys = @keys
        keys += [ instance_key(instance) ] if ! instance.nil?
        puts "call: #{keys.to_s}"
        @redis.evalsha @sha, keys, [ method, @name ] + args
    end

    def instance_key instance
        raise if instance.include? '/'
        "#{@name}/#{instance}"
    end

    def preprocess_udf udf
        lines = udf.split "\n"
        new_lines = []
        lines.each do |line|
            if !@debug
                m = /^(\s*)(log\(.*)$/.match(line)
                if m.nil?
                    new_lines << line
                else
                    new_lines << "#{m[1]}-- #{m[2]}"

                end
            else
                new_lines << line
            end
        end

        new_lines.join "\n"
    end

    def select_queue
        need = [
            @qlist_key,
            @qset_key
        ]
        @redis.evalsha @sha, need, [ 'select_queue' ]
    end

    def pop_from_queue queue
        need = [
            @qlist_key, #1
            @qset_key, #2
            @count_key, #3
            queue #4
        ]
        @redis.evalsha @sha, need, [ 'pop' ]
    end

    public
    def initialize redis, name, debug: false
        raise if name.include? '/'
        @debug = debug
        @name = name
        @qlist_key = "q_list_#{name}"
        @qset_key = "q_set_#{name}"
        @count_key = "q_count_#{name}"
        @keys = [ @qlist_key, "q_set_#{name}", @count_key ]
        @redis = redis
        udf = IO.read('udf/queues.lua')
        udf = preprocess_udf udf
        @sha = @redis.script :load, udf
    end


    attr_reader :sha

    def len
        @redis.call 'get', @count_key
    end

    def clear instance
        queue = instance_key(instance)
        need = [
            @qlist_key, #1
            @qset_key, #2
            @count_key, #3
            queue #4
        ]
        @redis.evalsha @sha, need, [ 'clear' ]
    end

    def push instance, data
        queue = instance_key(instance)
        need = [
            @qlist_key, #1
            @qset_key, #2
            @count_key, #3
            queue #4
        ]
        @redis.evalsha @sha, need, [ 'push', data ]
    end



    def put_counter instance, data, count
        raise if ! count.is_a? Numeric
        queue = instance_key(instance)
        need = [
            @qlist_key, #1
            @qset_key, #2
            @count_key, #3
            queue #4
        ]
        @redis.evalsha @sha, need, [ 'put_counter', data, count ]
    end


    def pop safety_lock = 10
        raise if safety_lock == 0
        loop do
            return nil if safety_lock == 0
            safety_lock -= 1
            queue = select_queue
            return nil if queue.nil?
            rv = pop_from_queue queue
            if ! rv.nil?
                s = rv[0].split('/')[1]
                rv[0] = s
                return rv
            end
        end
    end

    def len instance=nil
        raise if ! count.is_a? Numeric
        if instance.nil?
            queue = nil

        else
            queue = instance_key(instance)
        end
        need = [
            @qlist_key, #1
            @qset_key, #2
            @count_key, #3
            queue #4
        ]
        @redis.evalsha @sha, need, [ 'put_counter', data, count ]

    end

    def clear_log
        @redis.call 'del', '_debug'
    end
end

redis = Redis.new
redis.call 'FLUSHALL'
q = Queue28.new(redis, 'smrt', debug: true)
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
end

puts q.len
