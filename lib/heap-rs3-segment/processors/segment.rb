module HeapRS3Segment
  module Processors
    class Segment
      attr_accessor :analytics, :max_queue_size

      def initialize(*args)
        @analytics = ::Segment::Analytics.new(*args)
        
        @max_queue_size = @analytics.
          instance_variable_get('@client').
          instance_variable_get('@max_queue_size')
      end
      
      def check_flush_queue!
        if @analytics.queued_messages >= @max_queue_size
          t = Time.now
          puts "Max queue size reached - #{@processor.queued_messages}, flushing"
          @analytics.flush
          diff = Time.now - t
          rate = (@max_queue_size / diff).to_i
          puts "Flush done in #{diff} seconds (#{rate} req/sec), continue"
        end
      end

      def track(attrs)
        check_flush_queue!
        @analytics.track(attrs)
      end
      
      def identify(attrs)
        check_flush_queue!
        @analytics.identify(attrs)
      end
      
      def page(attrs)
        check_flush_queue!
        @analytics.page(attrs)
      end
      
      def alias(attrs)
        check_flush_queue!
        @analytics.alias(attrs)
      end
    end
  end
end