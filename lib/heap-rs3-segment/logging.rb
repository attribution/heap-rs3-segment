require 'time'
require 'logger'

module HeapRS3Segment
  module Logging
    class Pretty < Logger::Formatter
      def call(severity, time, program_name, message)
         "#{time.utc.iso8601} #{severity} #{message}\n"
      end
    end

    def self.initialize_logger(log_target = STDOUT)
      @logger = Logger.new(log_target)
      @logger.level = Logger::DEBUG # Logger::INFO
      @logger.formatter = Pretty.new
      @logger
    end

    def self.logger
      defined?(@logger) ? @logger : initialize_logger
    end

    def self.logger=(log)
      @logger = (log ? log : Logger.new(File::NULL))
    end

    def logger
      self.class.logger
    end
  end
end
