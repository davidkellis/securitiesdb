require 'singleton'

class EodBarLoader < TimeSeriesMapLoader
  include Singleton

  def self.get(security, datestamp)
    instance.get(security, datestamp)
  end


  def initialize()
    super(50)  # 50 TimeSeriesMap objects - one per unique (Security, year) pair
  end

  protected

  # compute cache key that identifies a unique (TimeSeries, year) pair
  def cache_key(security, datestamp)
    year = datestamp / 10000
    "#{security.id}-#{year}"
  end

  # query the database for a year's worth of EodBars associated with <security> and covering the date given by <datestamp>
  def find_observations(security, datestamp)
    year, _, _ = Date.datestamp_components(datestamp)
    first_datestamp_of_year = Date.build_datestamp(year, 1, 1)
    last_datestamp_of_year = Date.build_datestamp(year, 12, 31)
    security.eod_bars_dataset.where { (date >= first_datestamp_of_year) & (date <= last_datestamp_of_year) }.to_a
  end

  def extract_observation_time(eod_bar)
    eod_bar.date
  end

  def extract_observation_value(eod_bar)
    eod_bar
  end
end
