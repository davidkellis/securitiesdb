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
    key = @navigable_map.floorKey(time)    # floorKey returns the greatest key less than or equal to the given key, or null if there is no such key.
    @navigable_map[key] if key
  end

  def latest_value_earlier_than(time)
    key = @navigable_map.lowerKey(time)    # lowerKey returns the greatest key strictly less than the given key, or null if there is no such key.
    @navigable_map[key] if key
  end

  def earliest_value_at_or_later_than(time)
    key = @navigable_map.ceilingKey(time)  # ceilingKey returns the least key greater than or equal to the given key, or null if there is no such key.
    @navigable_map[key] if key
  end

  def earliest_value_later_than(time)
    key = @navigable_map.higherKey(time)   # higherKey returns the least key strictly greater than the given key, or null if there is no such key.
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
      "EOD Open #{@security.name} (id=#{@security.id})"
    end

    def observe(timestamp)
      datestamp = Date.timestamp_to_datestamp(timestamp)
      tsmap = EodBarLoader.get(@security, datestamp)
      eod_bar = tsmap.latest_value_at_or_earlier_than(datestamp)
      eod_bar.open.to_f if eod_bar
    end
  end

  class EodBarHigh < Variable
    def initialize(security)
      @security = security
    end

    def name
      "EOD High #{@security.name} (id=#{@security.id})"
    end

    def observe(timestamp)
      datestamp = Date.timestamp_to_datestamp(timestamp)
      tsmap = EodBarLoader.get(@security, datestamp)
      eod_bar = tsmap.latest_value_at_or_earlier_than(datestamp)
      eod_bar.high.to_f if eod_bar
    end
  end

  class EodBarLow < Variable
    def initialize(security)
      @security = security
    end

    def name
      "EOD Low #{@security.name} (id=#{@security.id})"
    end

    def observe(timestamp)
      datestamp = Date.timestamp_to_datestamp(timestamp)
      tsmap = EodBarLoader.get(@security, datestamp)
      eod_bar = tsmap.latest_value_at_or_earlier_than(datestamp)
      eod_bar.low.to_f if eod_bar
    end
  end

  class EodBarClose < Variable
    def initialize(security)
      @security = security
    end

    def name
      "EOD Close #{@security.name} (id=#{@security.id})"
    end

    def observe(timestamp)
      datestamp = Date.timestamp_to_datestamp(timestamp)
      tsmap = EodBarLoader.get(@security, datestamp)
      eod_bar = tsmap.latest_value_at_or_earlier_than(datestamp)
      eod_bar.close.to_f if eod_bar
    end
  end

  class EodBarVolume < Variable
    def initialize(security)
      @security = security
    end

    def name
      "EOD Volume #{@security.name} (id=#{@security.id})"
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
  def initialize
    @variables = []
  end

  def add_column(variable)
    @variables << variable
  end

  # blk is a block that takes 2 arguments: time and row
  def each_nonfiltered_row(time_series, slice_size = 30, &blk)
    if block_given?
      time_series.each_slice(slice_size).each do |time_series_slice|
        time_series_slice.each do |time|
          row = @variables.map {|variable| variable.observe(time) }
          blk.call(time, row)
        end
      end
    else
      enum_for(:each_nonfiltered_row, time_series, slice_size)
    end
  end

  # same as #each_nonfiltered_row except that rows containing any nil values are filtered out
  # blk is a block that takes 2 arguments: time and row
  def each_filtered_row(time_series, slice_size = 30, &blk)
    if block_given?
      each_nonfiltered_row(time_series, slice_size) do |time, row|
        blk.call(time, row) unless row.any?(&:nil?)
      end
    else
      enum_for(:each_filtered_row, time_series, slice_size)
    end
  end

  # row_enum_fn is either :each_nonfiltered_row, or :each_filtered_row
  # blk is a block that takes 1 argument: row
  def each_row(time_series, include_column_headers = true, include_date_column = true, row_enum_fn = :each_filtered_row, &blk)
    if block_given?
      if include_column_headers
        column_headers = @variables.map(&:name)
        column_headers.unshift("Date") if include_date_column
        blk.call(column_headers)
      end
      self.send(row_enum_fn, time_series) do |time, row|
        row.unshift(time) if row && include_date_column
        blk.call(row)
      end
    else
      enum_for(:each_row, time_series, include_column_headers, include_date_column, row_enum_fn)
    end
  end

  # row_enum_fn is either :each_nonfiltered_row, or :each_filtered_row
  def to_a(time_series, include_column_headers = true, include_date_column = true, row_enum_fn = :each_filtered_row)
    each_row(time_series, include_column_headers, include_date_column, row_enum_fn).to_a
  end

  # row_enum_fn is either :each_nonfiltered_row, or :each_filtered_row
  def save_csv(filepath, time_series, include_column_headers = true, include_date_column = true, row_enum_fn = :each_filtered_row)
    File.open(filepath, "w+") do |f|
      first_row = include_column_headers
      each_row(time_series, include_column_headers, include_date_column, row_enum_fn) do |row|
        if first_row
          # per page 2 of RFC-4180, "If double-quotes are used to enclose fields, then a double-quote appearing inside
          # a field must be escaped by preceding it with another double quote."
          header_row = row.
            map {|col_value| col_value.gsub('"', '""') }.   # escape each double quote with a preceeding double quote
            map {|col_value| "\"#{col_value}\"" }.          # enclose each header field within double quotes
            join(',')
          f.puts(header_row)
          first_row = false
        else
          f.puts(row.join(','))
        end
      end
    end
  end
end
