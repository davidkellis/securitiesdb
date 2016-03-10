require 'forwardable'
require 'simstring_pure'

class SecurityNameDatabase
  class << self
    extend Forwardable

    def_delegators :instance, :add, :save, :search, :ranked_search

    def instance
      @instance
    end

    def configure(file_path)
      @instance = self.new(file_path)
    end
  end

  def initialize(file_path)
    @file_path = file_path
    @db = if File.exist?(@file_path)
      SimString::Database.load(@file_path)
    else
      SimString::Database.new
    end
    @matcher = SimString::StringMatcher.new(@db, SimString::CosineMeasure.new)
  end

  def add(company_name)
    @db.add(company_name)
  end

  def save
    @db.save(file_path)
  end

  def search(query_string, alpha)
    @matcher.search(query_string, alpha)
  end

  def ranked_search(query_string, alpha)
    @matcher.ranked_search(query_string, alpha)
  end
end
