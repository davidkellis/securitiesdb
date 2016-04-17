require 'java'
require 'singleton'

require 'lru_redux'

class LruCache
  def initialize(size)
    @cache = LruRedux::Cache.new(size)
  end

  def get(key)
    @cache[key]
  end

  def set(key, value)
    @cache[key] = value
  end

  def get_or_set(key, &blk)
    @cache.getset(key, &blk)
  end
end

class TimeSeriesMap
  def initialize
    @navigable_map = java.util.TreeMap.new
  end

  def add(time, value)
    @navigable_map.put(time, value)
  end

  def remove(time)
    @navigable_map.remove(time)
  end

  def [](time)
    @navigable_map[time]
  end

  def get(time)
    @navigable_map[time]
  end

  def latest_value_at_or_earlier_than(time)
    key = navigable_map.floorKey(time)    # floorKey returns the greatest key less than or equal to the given key, or null if there is no such key.
    @navigable_map[key] if key
  end

  def latest_value_earlier_than(time)
    key = navigable_map.lowerKey(time)    # lowerKey returns the greatest key strictly less than the given key, or null if there is no such key.
    @navigable_map[key] if key
  end

  def earliest_value_at_or_later_than(time)
    key = navigable_map.ceilingKey(time)  # ceilingKey returns the least key greater than or equal to the given key, or null if there is no such key.
    @navigable_map[key] if key
  end

  def earliest_value_later_than(time)
    key = navigable_map.higherKey(time)   # higherKey returns the least key strictly greater than the given key, or null if there is no such key.
    @navigable_map[key] if key
  end
end

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
    cache.get(key) || load_eod_bars_into_cache(security, datestamp)
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
    security.eod_bars_dataset.where { (first_datestamp_of_month <= date) & (date <= last_possible_datestamp_of_month) }.to_a
  end
end

def TimeSeriesDailyObservationLoader
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

class Variable
  def name
    raise "#{self.class.name}#name not implemented."
  end

  # timestamp is an integer timestamp of the form yyyymmddHHMMSS
  def observe(timestamp)
    raise "#{self.class.name}#observe not implemented."
  end
end

module Variables
  class EodBarOpen < Variable
    def initialize(security)
      @security = security
    end

    def name
      "EOD Open #{@security.name} (id=#{security.id})"
    end

    def observe(timestamp)
      datestamp = Date.timestamp_to_datestamp(timestamp)
      tsmap = EodBarLoader.get(@security, datestamp)
      eod_bar = tsmap.latest_value_at_or_earlier_than(datestamp)
      eod_bar.open if eod_bar
    end
  end

  class EodBarHigh < Variable
    def initialize(security)
      @security = security
    end

    def name
      "EOD High #{@security.name} (id=#{security.id})"
    end

    def observe(timestamp)
      datestamp = Date.timestamp_to_datestamp(timestamp)
      tsmap = EodBarLoader.get(@security, datestamp)
      eod_bar = tsmap.latest_value_at_or_earlier_than(datestamp)
      eod_bar.high if eod_bar
    end
  end

  class EodBarLow < Variable
    def initialize(security)
      @security = security
    end

    def name
      "EOD Low #{@security.name} (id=#{security.id})"
    end

    def observe(timestamp)
      datestamp = Date.timestamp_to_datestamp(timestamp)
      tsmap = EodBarLoader.get(@security, datestamp)
      eod_bar = tsmap.latest_value_at_or_earlier_than(datestamp)
      eod_bar.low if eod_bar
    end
  end

  class EodBarClose < Variable
    def initialize(security)
      @security = security
    end

    def name
      "EOD Close #{@security.name} (id=#{security.id})"
    end

    def observe(timestamp)
      datestamp = Date.timestamp_to_datestamp(timestamp)
      tsmap = EodBarLoader.get(@security, datestamp)
      eod_bar = tsmap.latest_value_at_or_earlier_than(datestamp)
      eod_bar.close if eod_bar
    end
  end

  class EodBarVolume < Variable
    def initialize(security)
      @security = security
    end

    def name
      "EOD Volume #{@security.name} (id=#{security.id})"
    end

    def observe(timestamp)
      datestamp = Date.timestamp_to_datestamp(timestamp)
      tsmap = EodBarLoader.get(@security, datestamp)
      eod_bar = tsmap.latest_value_at_or_earlier_than(datestamp)
      eod_bar.volume if eod_bar
    end
  end

  class DailyTimeSeriesObservation < Variable
    def initialize(time_series)
      @time_series = time_series
    end

    def name
      "#{@time_series.database}/#{@time_series.dataset} - #{@time_series.name}"
    end

    def observe(timestamp)
      datestamp = Date.timestamp_to_datestamp(timestamp)
      tsmap = TimeSeriesDailyObservationLoader.get(@time_series, datestamp)
      tsmap.latest_value_at_or_earlier_than(datestamp)
    end
  end
end

class TimeSeriesTable
  # missing_values_behavior must be one of the following symbols:
  #   :omit - do not fill in missing values
  #   :most_recent - fill in missing values using the most recent prior observation
  #   :most_recent_or_omit - fill in missing values using the most recent prior observation; if most recent value doesn't exist, omit
  Column = Struct.new(:variable, :missing_values_behavior)

  attr_accessor :columns

  def initialize
    @columns = []
  end

  # missing_values_behavior must be one of the following symbols:
  #   :omit - do not fill in missing values
  #   :most_recent - fill in missing values using the most recent prior observation (if most recent value doesn't exist, use nil as placeholder)
  #   :most_recent_or_omit - fill in missing values using the most recent prior observation; if most recent value doesn't exist, omit
  def add_column(variable, missing_values_behavior = :omit)
    @columns << Column.new(variable, missing_values_behavior)
  end

  def each_row(time_series, slice_size = 30, &blk)
    if block_given?
      time_series.each_slice(slice_size).each do |time_series_slice|
        time_series_slice.each do |time|
          blk.call(@columns.map {|column| column.variable.observe(time) })
        end
      end
    else
      enum_for(:each_row, time_series, slice_size)
    end
  end

  def to_a(include_column_headers = true, include_date_column = true)
    common_dates = identify_common_dates_among_core_columns
    sorted_common_dates = common_dates.to_a.sort
    table = each_row(sorted_common_dates).map do |datestamp|
      row = columns.map do |column|
        case column.missing_values_behavior
        when :omit
          column.date_value_map[datestamp]    # this is assured to return a value because sorted_common_dates only contains datestamps that are common to all the "none" columns
        when :most_recent
          most_recent_value(column.date_value_map, datestamp)
        when :most_recent_or_omit
          most_recent_value(column.date_value_map, datestamp) || break
        else
          raise "Unknown missing value behavior, \"#{column.missing_values_behavior}\", for column #{column.name}."
        end
      end
      row.unshift(datestamp) if row && include_date_column
      row
    end
    table.compact!    # remove any rows that were nil due to being omitted
    if include_column_headers
      column_headers = columns.map {|column| column.variable.name }
      column_headers.unshift("Date") if include_date_column
      table.unshift(column_headers)
    end
    table
  end

  def to_csv(include_column_headers = true, include_date_column = true)
    to_a(include_column_headers, include_date_column).map {|row| row.join(',') }.join("\n")
  end

  private

  def no_fill_columns
    columns.select {|col| col.missing_values_behavior == :omit }
  end

  def identify_common_dates_among_core_columns
    core_columns = no_fill_columns
    if !core_columns.empty?
      first_key_set = core_columns.first.date_value_map.keySet.to_set
      core_columns.reduce(first_key_set) {|common_key_set, column| common_key_set & column.date_value_map.keySet.to_set }
    end
  end

  def most_recent_value(navigable_map, datestamp)
    key = navigable_map.floorKey(datestamp)
    navigable_map[key] if key
  end
end
