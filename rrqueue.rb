class RRQueue

    # path to lua script
    def lua_path
        File.expand_path("../udf/queues.lua", __FILE__)
    end

    def lua_sha1_path
        if @debug
            "#{lua_path}.sha1_debug"
        else
            "#{lua_path}.sha1_relase"
        end
    end

    def cached_sha1
        IO.read lua_sha1_path
    rescue
        nil
    end

    # redis.evalsha wrapper
    def _eval need, args
        # DLOG "RRQueue-eval need=#{need}, args=#{args}"
        if @sha.nil?
            # try to get sha1 from cahe
            @sha = cached_sha1
            update_udf if @sha.nil?
            raise if @sha.nil?
        end
        begin
            return @redis.evalsha @sha, need, args
        rescue ::Redis::CommandError => e
            # handle case of invalid cache
            update_udf
            return @redis.evalsha @sha, need, args
        end
    end

    def instance_key instance
        # instance must not contain '/' character!
        raise if instance.include? '/'
        "#{@name}/#{instance}"
    end

    # comment out all the log(...) commands in lua code
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
        _eval need, [ 'select_queue' ]
    end

    def pop_from_queue queue
        need = [
            @qlist_key, #1
            @qset_key, #2
            @count_key, #3
            queue #4
        ]
        _eval need, [ 'pop' ]
    end

    public
    def initialize redis, name, debug: false
        raise if name.include? '/'
        @debug = debug
        @name = name
        @total_key = "q_total_#{name}"
        @qlist_key = "q_list_#{name}"
        @qset_key = "q_set_#{name}"
        @count_key = "q_count_#{name}"
        @keys = [ @qlist_key, "q_set_#{name}", @count_key ]
        @redis = redis
   end


    attr_reader :sha

    def lua_code
        udf = IO.read(lua_path)
        preprocess_udf udf
    end

    def update_udf
        @sha = @redis.script(:load, lua_code)
        IO.write(lua_sha1_path, @sha)
        @sha
    end


    def ping
        _eval [ 'x' ], ['ping']
    end

    def len
        rv = @redis.call 'get', @count_key
        rv = 0 if rv.nil?
        rv
    end

    def total
        rv = @redis.call 'get', @total_key
        rv = 0 if rv.nil?
        rv
    end


    def clear instance
        queue = instance_key(instance)
        need = [
            @qlist_key, #1
            @qset_key, #2
            @count_key, #3
            queue #4
        ]
        _eval need, [ 'clear' ]
    end

    def push instance, data
        queue = instance_key(instance)
        need = [
            @qlist_key, #1
            @qset_key, #2
            @count_key, #3
            queue, #4
            @total_key # 5
        ]
        _eval need, [ 'push', data ]
    end

    def len_of instance
        queue = instance_key(instance)

        need = [ queue ]
        rv = _eval need, [ 'len_of' ]
        Integer(rv)
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
        _eval need, [ 'put_counter', data, count ]
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

    def clear_log
        @redis.call 'del', '_debug'
    end

    def queues
        rv = []
        raw = @redis.call 'lrange', @qlist_key, 0, 10000
        raw.each do |x|
            len = @redis.call 'llen', x
            next if len == 0
            rv << {
                q: x.split('/')[1][1..-1],
                l: len
            }
        end
        rv
    end
end
