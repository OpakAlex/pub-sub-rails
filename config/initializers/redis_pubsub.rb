require 'stringio'
require 'rubygems'
require 'eventmachine'

class Redis
  class PubSub < EM::Connection
    def self.connect host = nil, port = nil
      @host = (host || ENV['REDIS_HOST'] || 'localhost')
      @port = (port || ENV['REDIS_PORT'] || 6379).to_i

      EM.connect @host, @port, self
    end

    def post_init; @blocks = {} end
    def publish(channel, msg); call_command('publish', channel, msg) end

    def subscribe(*channels, &blk)
      channels.each { |c| @blocks[c.to_s] = blk }

      call_command('subscribe', *channels)
    end

    def unsubscribe *channels
      channels.each { |c| @blocks[c.to_s] = nil }

      call_command('unsubscribe', *channels)
    end

    def receive_data(data)
      buffer = StringIO.new(data)
      begin
        parts = read_response(buffer)
        if parts.is_a?(Array)
          ret = @blocks[parts[1]].call(parts)

          close_connection if ret === false
        end
      end while !buffer.eof?
    end

    private

    def read_response(buffer)
      type = buffer.read(1)

      if type == ':'
        buffer.gets.to_i
      elsif type == '*'
        size = buffer.gets.to_i
        parts = size.times.map { read_object(buffer) }
      else
        raise "unsupported response type"
      end
    end

    def read_object(data)
      type = data.read(1)

      if type == ':' # integer
        data.gets.to_i
      elsif type == '$'
        size = data.gets
        str = data.read(size.to_i)

        data.read(2) # crlf
        str
      else
        raise "read for object of type #{type} not implemented"
      end
    end

    # only support multi-bulk
    def call_command(*args)
      command = "*#{args.size}\r\n"

      args.each { |a|
        command << "$#{a.to_s.size}\r\n"
        command << a.to_s
        command << "\r\n"
      }
      send_data command
    end
  end
end

init = proc {
  sub = Redis::PubSub.connect
  pub = Redis::PubSub.connect

  sub.subscribe("chat") do |t, c, m|
    puts m #=> "hello"
  end

  pub.publish("chat", "hello")
}

EM.next_tick(init)
