require 'singleton'

class TimeSeriesDailyObservationLoader < TimeSeriesMapLoader
  include Singleton

  def self.get(time_series, datestamp)
    instance.get(time_series, datestamp)
  end


  def initialize()
    super(50)  # 50 TimeSeriesMap objects - one per unique (TimeSeries, year) pair
  end

  protected

  # compute cache key that identifies a unique (TimeSeries, year) pair
  def cache_key(time_series, datestamp)
    year = datestamp / 10000
    "#{time_series.id}-#{year}"
  end

  # query the database for a year's worth of daily observations associated with <time_series> and covering the date given by <datestamp>
  def find_observations(time_series, datestamp)
    year, _, _ = Date.datestamp_components(datestamp)
    first_datestamp_of_year = Date.build_datestamp(year, 1, 1)
    last_datestamp_of_year = Date.build_datestamp(year, 12, 31)
    time_series.daily_observations_dataset.where { (date >= first_datestamp_of_year) & (date <= last_datestamp_of_year) }.to_a
  end

  def extract_observation_time(observation)
    observation.date
  end

  def extract_observation_value(observation)
    observation.value
  end
end

class TimeSeriesWeeklyObservationLoader < TimeSeriesMapLoader
  include Singleton

  def self.get(time_series, datestamp)
    instance.get(time_series, datestamp)
  end


  def initialize()
    super(100)  # 100 TimeSeriesMap objects - one per unique (TimeSeries, year) pair
  end

  protected

  # compute cache key that identifies a unique (TimeSeries, year) pair
  def cache_key(time_series, datestamp)
    year = datestamp / 10000
    "#{time_series.id}-#{year}"
  end

  # query the database for a year's worth of weekly observations associated with <time_series> and covering the date given by <datestamp>
  def find_observations(time_series, datestamp)
    year, _, _ = Date.datestamp_components(datestamp)
    first_datestamp_of_year = Date.build_datestamp(year, 1, 1)
    last_datestamp_of_year = Date.build_datestamp(year, 12, 31)
    time_series.weekly_observations_dataset.where { (date >= first_datestamp_of_year) & (date <= last_datestamp_of_year) }.to_a
  end

  def extract_observation_time(observation)
    observation.date
  end

  def extract_observation_value(observation)
    observation.value
  end
end

class TimeSeriesMonthlyObservationLoader < TimeSeriesMapLoader
  include Singleton

  def self.get(time_series, datestamp)
    instance.get(time_series, datestamp)
  end


  def initialize()
    super(100)  # 100 TimeSeriesMap objects - one per unique TimeSeries
  end

  protected

  # compute cache key that identifies a unique TimeSeries
  def cache_key(time_series, datestamp)
    time_series.id
  end

  # query the database for all monthly observations associated with <time_series>
  def find_observations(time_series, datestamp)
    time_series.monthly_observations.to_a
  end

  def extract_observation_time(observation)
    observation.date
  end

  def extract_observation_value(observation)
    observation.value
  end
end


class TimeSeriesQuarterlyObservationLoader < TimeSeriesMapLoader
  include Singleton

  def self.get(time_series, datestamp)
    instance.get(time_series, datestamp)
  end


  def initialize()
    super(100)  # 100 TimeSeriesMap objects - one per unique TimeSeries
  end

  protected

  # compute cache key that identifies a unique TimeSeries
  def cache_key(time_series, datestamp)
    time_series.id
  end

  # query the database for all quarterly observations associated with <time_series>
  def find_observations(time_series, datestamp)
    time_series.quarterly_observations.to_a
  end

  def extract_observation_time(observation)
    observation.date
  end

  def extract_observation_value(observation)
    observation.value
  end
end

class TimeSeriesYearlyObservationLoader < TimeSeriesMapLoader
  include Singleton

  def self.get(time_series, datestamp)
    instance.get(time_series, datestamp)
  end


  def initialize()
    super(100)  # 100 TimeSeriesMap objects - one per unique TimeSeries
  end

  protected

  # compute cache key that identifies a unique TimeSeries
  def cache_key(time_series, datestamp)
    time_series.id
  end

  # query the database for all yearly observations associated with <time_series>
  def find_observations(time_series, datestamp)
    time_series.yearly_observations.to_a
  end

  def extract_observation_time(observation)
    observation.date
  end

  def extract_observation_value(observation)
    observation.value
  end
end

class TimeSeriesObservationLoader
  def self.get(time_series, datestamp)
    case time_series.update_frequency.label
    when UpdateFrequency::DAILY
      TimeSeriesDailyObservationLoader.get(time_series, datestamp)
    when UpdateFrequency::WEEKLY
      TimeSeriesWeeklyObservationLoader.get(time_series, datestamp)
    when UpdateFrequency::MONTHLY
      TimeSeriesMonthlyObservationLoader.get(time_series, datestamp)
    when UpdateFrequency::QUARTERLY
      TimeSeriesQuarterlyObservationLoader.get(time_series, datestamp)
    when UpdateFrequency::YEARLY
      TimeSeriesYearlyObservationLoader.get(time_series, datestamp)
    when UpdateFrequency::IRREGULAR
      raise "Not implemented."
    end
  end
end
