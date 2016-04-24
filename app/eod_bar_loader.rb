require 'singleton'

class EodBarLoader
  include Singleton

  class << self
    extend Forwardable
    def_delegators :instance, :get
  end

  attr_accessor :cache

  def initialize()
    self.cache = LruCache.new(100)    # 100 TimeSeriesMap objects - one per unique (security, month) pair
  end

  # returns a TimeSeriesMap[integer datestamp, EodBar] containing the eod bars for the month in which <datestamp> falls, for the given security
  # datestamp is an integer of the form yyyymmdd
  def get(security, datestamp)
    monthstamp = Date.datestamp_to_monthstamp(datestamp)  # datestamp is of the form yyyymmdd; monthstamp is of the form yyyymm
    key = cache_key(security, monthstamp)
    cache.get(key) || load_eod_bars_into_cache(security, monthstamp)
  end

  private

  def cache_key(security, monthstamp)
    "#{security.id}-#{monthstamp}"
  end

  def load_eod_bars_into_cache(security, monthstamp)
    key = cache_key(security, monthstamp)
    time_series_map = load_eod_bars_into_time_series_map(security, monthstamp)
    cache.set(key, time_series_map) if time_series_map
    time_series_map
  end

  # returns TimeSeriesMap[integer datestamp, EodBar] for the month's worth of eod bars
  def load_eod_bars_into_time_series_map(security, monthstamp)
    map = TimeSeriesMap.new
    eod_bars = find_eod_bars_in_month(security, monthstamp)
    eod_bars.each {|eod_bar| map.add(eod_bar.date, eod_bar) }
    map
  end

  # query the database for a month's worth of EodBars
  def find_eod_bars_in_month(security, monthstamp)
    first_datestamp_of_month = monthstamp * 100 + 1
    last_possible_datestamp_of_month = monthstamp * 100 + 31
    security.eod_bars_dataset.where { (date >= first_datestamp_of_month) & (date <= last_possible_datestamp_of_month) }.to_a
  end
end
