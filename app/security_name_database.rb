require 'fileutils'
require 'forwardable'
require 'singleton'

require 'lru_redux'
require 'simstring_pure'
require 'text'

class SecurityNameDatabase
  class << self
    # uses Double Metaphone (and perhaps in the future Metaphone 3 - see https://github.com/OpenRefine/OpenRefine/blob/master/main/src/com/google/refine/clustering/binning/Metaphone3.java)
    def phonetic_key(company_name)
      words = company_name.gsub(/\s+/m, ' ').strip.split(" ")
      phonetic_words = words.map {|word| Text::Metaphone.double_metaphone(word).first }
      phonetic_words.join(" ")
    end
  end

  def initialize(file_path = nil)
    @file_path = file_path
    @db = if @file_path && File.exist?(@file_path)
      SimString::Database.load(@file_path)
    else
      ngram_builder = SimString::NGramBuilder.new(3)
      SimString::Database.new(ngram_builder)
    end
    @matcher = SimString::StringMatcher.new(@db, SimString::CosineMeasure.new)
  end

  def add(company_name)
    @db.add(company_name)
  end

  def save
    @db.save(@file_path) if @file_path
  end

  def search(query_string, alpha)
    @matcher.search(query_string, alpha)
  end

  def ranked_search(query_string, alpha)
    @matcher.ranked_search(query_string, alpha)
  end
end

class SecurityNameDatabaseFactory
  class << self
    extend Forwardable

    def_delegators :instance, :get

    def instance
      @instance
    end

    def configure(data_dir)
      @instance = self.new(data_dir)
    end
  end

  def initialize(data_directory_path)
    FileUtils.mkdir_p(data_directory_path)
    @data_directory = data_directory_path
  end

  # database_name should be a lower case alphanumeric underscored or hypenated name, e.g. a file name like "company_names" or "option_identifiers";
  def get(database_name)
    path = File.join(@data_directory, "#{database_name}.db")
    SecurityNameDatabase.new(path)
  end
end

class SecurityNameDatabaseRegistry
  include Singleton

  class << self
    extend Forwardable

    def_delegators :instance, :get, :save_all
  end

  def initialize
    # @databases = LruRedux::Cache.new(10, ->(db){ db.save })   # second argument is an item eviction callback - we want to save the DB immediately before it is evicted from the LRU cache
    @databases = {}
  end

  def get(database_name)
    @databases[database_name] ||= SecurityNameDatabaseFactory.get(database_name)
  end

  def save_all
    @databases.each {|db_name, db| db.save }
  end
end
