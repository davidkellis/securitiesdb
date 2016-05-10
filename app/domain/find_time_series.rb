class FindTimeSeriesDailyObservation
  class << self
    def at_or_earlier_than(time_series, datestamp)
      # first attempt to find the observation - search this year's worth of observations
      tsmap = TimeSeriesDailyObservationLoader.get(time_series, datestamp)
      observation = tsmap.latest_value_at_or_earlier_than(datestamp)
      return observation if observation

      # second attempt to find the observation - search previous year's worth of observations
      year, month, day = *Date.datestamp_components(datestamp)
      tsmap = TimeSeriesDailyObservationLoader.get(time_series, Date.build_datestamp(year - 1, month, day))
      observation = tsmap.latest_value_at_or_earlier_than(datestamp)
      return observation if observation

      # third attempt to find the observation - query DB for latest daily observation observed on or earlier than <datestamp>
      time_series.daily_observations_dataset.
        where { date <= datestamp }.
        order(Sequel.desc(:date)).
        limit(1).
        first
    end
  end
end

class FindTimeSeriesWeeklyObservation
  class << self
    def at_or_earlier_than(time_series, datestamp)
      # first attempt to find the observation - search this year's worth of observations
      tsmap = TimeSeriesWeeklyObservationLoader.get(time_series, datestamp)
      observation = tsmap.latest_value_at_or_earlier_than(datestamp)
      return observation if observation

      # second attempt to find the observation - search previous year's worth of observations
      year, month, day = *Date.datestamp_components(datestamp)
      tsmap = TimeSeriesWeeklyObservationLoader.get(time_series, Date.build_datestamp(year - 1, month, day))
      observation = tsmap.latest_value_at_or_earlier_than(datestamp)
      return observation if observation

      # third attempt to find the observation - query DB for latest weekly observation observed on or earlier than <datestamp>
      time_series.daily_observations_dataset.
        where { date <= datestamp }.
        order(Sequel.desc(:date)).
        limit(1).
        first
    end
  end
end

class FindTimeSeriesMonthlyObservation
  class << self
    def at_or_earlier_than(time_series, datestamp)
      tsmap = TimeSeriesMonthlyObservationLoader.get(time_series, datestamp)
      tsmap.latest_value_at_or_earlier_than(datestamp)
    end
  end
end

class FindTimeSeriesQuarterlyObservation
  class << self
    def at_or_earlier_than(time_series, datestamp)
      tsmap = TimeSeriesQuarterlyObservationLoader.get(time_series, datestamp)
      tsmap.latest_value_at_or_earlier_than(datestamp)
    end
  end
end

class FindTimeSeriesYearlyObservation
  class << self
    def at_or_earlier_than(time_series, datestamp)
      tsmap = TimeSeriesYearlyObservationLoader.get(time_series, datestamp)
      tsmap.latest_value_at_or_earlier_than(datestamp)
    end
  end
end
