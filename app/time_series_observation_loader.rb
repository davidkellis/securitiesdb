require 'singleton'

class TimeSeriesDailyObservationLoader
  include Singleton

  class << self
    extend Forwardable
    def_delegators :instance, :get
  end

  attr_accessor :cache

  def initialize()
    self.cache = LruCache.new(100)    # 100 TimeSeriesMap objects - one per unique (TimeSeries, month) pair
  end

  # returns a TimeSeriesMap[integer datestamp, DailyObservation] containing the daily observations for the month in
  # which <datestamp> falls, for the given TimeSeries
  # datestamp is an integer of the form yyyymmdd
  def get(time_series, datestamp)
    monthstamp = Date.datestamp_to_monthstamp(datestamp)  # datestamp is of the form yyyymmdd; monthstamp is of the form yyyymm
    key = cache_key(time_series, monthstamp)
    cache.get(key) || load_observations_into_cache(time_series, monthstamp)
  end

  private

  def cache_key(time_series, monthstamp)
    "#{time_series.id}-#{monthstamp}"
  end

  def load_observations_into_cache(time_series, monthstamp)
    key = cache_key(time_series, monthstamp)
    time_series_map = load_observations_into_time_series_map(security, monthstamp)
    cache.set(key, time_series_map) if time_series_map
    time_series_map
  end

  # returns TimeSeriesMap[integer datestamp, DailyObservation] for the month's worth of daily observations
  def load_observations_into_time_series_map(time_series, monthstamp)
    map = TimeSeriesMap.new
    observations = find_observations_in_month(time_series, monthstamp)
    observations.each {|observation| map.add(observation.date, observation.value) }
    map
  end

  # query the database for a month's worth of DailyObservations
  def find_observations_in_month(time_series, monthstamp)
    first_datestamp_of_month = monthstamp * 100 + 1
    last_possible_datestamp_of_month = monthstamp * 100 + 31
    time_series.daily_observations_dataset.where { (first_datestamp_of_month <= date) & (date <= last_possible_datestamp_of_month) }.to_a
  end
end
