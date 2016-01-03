require 'forwardable'
require 'sequel'
require 'uri'

if RUBY_PLATFORM == "java"    # check to see if we're running jruby for JRuby
  require 'jdbc/postgres'
end

class Database
  attr_reader :connection

  class << self
    extend Forwardable

    def instance
      @instance ||= self.new
    end

    def_delegator :instance, :connect
    def_delegator :instance, :connection
  end

  def connect(connection_string, logger = Application.database_logger)
    @connection ||= begin
      uri = URI(connection_string)
      case uri.scheme
      when "postgres", "mysql", "sqlite"
        connection = Sequel.connect(connection_string, :logger => logger)
        Sequel::Model.raise_on_save_failure = true
        connection
      when "jdbc"
        connection = Sequel.connect(connection_string, :logger => logger)
        Sequel::Model.raise_on_save_failure = true
        connection
      else
        raise "There is no database adapter for the following connection scheme: #{uri.scheme}"
      end
    end
  end

  def disconnect
    connection.disconnect
  end
end
