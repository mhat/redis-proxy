class RedisProxy

  class Error   < RuntimeError; end
  class Warning < RuntimeError; end

  attr_accessor :debug
  alias :debug? :debug

  def initialize(opts={}) 
    @redis_client = Redis.new(opts)

    ## facts
    @timeout_after_seconds              = 0.5
    @seconds_to_wait_before_retry       =  90
    @consecutive_errors_to_mark_as_dead =   2

    ## state 
    @consecutive_errors_detected        =   0
    @marked_dead_at                     = nil
  end

  def method_missing(method, *args)
    if debug?
      Rails.logger.debug("RedisProxy #{method} #{args.inspect}")
    end

    ## if enough time has passed to try sending commands to redis again
    if marked_dead? 
      if marked_dead_and_ready_to_be_resuscitated? 
        mark_alive  
      else
        raise RedisProxy::Error, "Dead and not ready for resuscitation (#{@seconds_to_wait_before_retry - (Time.now.to_i - @marked_dead_at)})"
      end
    end

    begin 
      SystemTimer.timeout_after(@timeout_after_seconds) do 
        ret = @redis_client.send(method, *args)
        @consecutive_errors_detected = 0
        return ret 
      end
    rescue Errno::ECONNREFUSED,
           Errno::ECONNRESET,
           Errno::EPIPE,
           Errno::ECONNABORTED,
           Errno::EBADF,
           Errno::EAGAIN,
           Timeout::Error => e

      @consecutive_errors_detected += 1

      ## NOTE: system time and redis-rb don't play all that well together; a timer may interrupt 
      ##       a call while we're reading a response; in this case the next call to redis will 
      ##       get the response for the previous-interuppeted call. 
      ##         1. alter redis-rb to clear the response buffer before sending a command
      ##         2. force a disconnect, which is effective the same as (1) but with less 
      ##            ruby overhead at the cost of having to do anothing tcp-handshake. 
      @redis_client.client.disconnect 

      ## try again or mark the server as dead
      if not should_mark_as_dead? 
        log_exception(RedisProxy::Warning.new("Warning: Original Exception: #{e}"), method.to_s)
        retry 
      else
        mark_dead 
        re = RedisProxy::Error.new("Marked dead: Original Exception: #{e}", method.to_s)
        log_exception(re, method.to_s)
        raise(re)
      end
    end
  end

  def client
    return @redis_client
  end

  def mark_alive
    @marked_dead_at              = nil 
    @consecutive_errors_detected = 0
  end

  def mark_dead
    begin  
      @marked_dead_at = Time.now.to_i
    rescue
    end
  end

  def marked_dead?
    ! @marked_dead_at.nil?
  end

  def marked_dead_and_ready_to_be_resuscitated?
    (marked_dead?) and (Time.now.to_i - @marked_dead_at > @seconds_to_wait_before_retry)
  end

  def should_mark_as_dead?
    @consecutive_errors_detected >= @consecutive_errors_to_mark_as_dead
  end

  def inspect 
    "#{RedisProxy}"
  end
  

  def log_exception(e, method=nil)
    self.class.log_exception(e,method)
  end

  def self.log_exception(e, method=nil)
    LoggedException.create_from_exception(mock_controller(method), e)
  end

  def self.mock_controller(method=nil)
    OpenStruct.new(:name => self.name, :action_name => (method || "unknown"))
  end
 
end

