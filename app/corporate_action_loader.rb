require 'singleton'

class CorporateActionLoader
  include Singleton

  class << self
    extend Forwardable
    def_delegators :instance, :get
  end

  attr_accessor :cache

  def initialize()
    self.cache = LruCache.new(100)    # 100 TimeSeriesMap objects - one per security
  end

  # returns a TimeSeriesMap[integer datestamp, CorporateAction] containing the splits and dividends for the given security
  # datestamp is an integer of the form yyyymmdd
  def get(security, datestamp)
    key = cache_key(security)
    cache.get(key) || load_corporate_actions_into_cache(security)
  end

  private

  def cache_key(security)
    security.id
  end

  def load_corporate_actions_into_cache(security)
    key = cache_key(security)
    time_series_map = load_eod_bars_into_time_series_map(security)
    cache.set(key, time_series_map) if time_series_map
    time_series_map
  end

  # returns TimeSeriesMap[integer datestamp, CorporateAction] containing all the corporate actions for the given security
  def load_corporate_actions_into_time_series_map(security)
    map = TimeSeriesMap.new
    corporate_actions = security.corporate_actions
    corporate_actions.each {|corporate_action| map.add(eod_bar.ex_date, corporate_action) }
    map
  end
end
