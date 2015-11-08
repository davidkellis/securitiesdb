require 'forwardable'
require 'sequel'
require 'uri'

class Database
  attr_reader :connection

  class << self
    extend Forwardable

    def instance
      @instance ||= self.new
    end

    def_delegator :instance, :connect
  end

  def connect(connection_string, logger = Application.logger)
    @connection ||= begin
      uri = URI(connection_string)
      case uri.scheme
      when "postgres", "mysql", "sqlite"
        Sequel.connect(connection_string, :logger => logger)
        Sequel::Model.raise_on_save_failure = true
      else
        raise "There is no database adapter for the following connection scheme: #{uri.scheme}"
      end
    end
  end

  def disconnect
    connection.disconnect
  end
end
