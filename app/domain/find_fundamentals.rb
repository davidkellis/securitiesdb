class FindFundamentalDailyObservation
  class << self
    def at_or_earlier_than(fundamental_dataset, datestamp)
      time_series = fundamental_dataset.time_series
      FindTimeSeriesDailyObservation.at_or_earlier_than(time_series, datestamp)
    end
  end
end

class FindFundamentalWeeklyObservation
  class << self
    def at_or_earlier_than(fundamental_dataset, datestamp)
      time_series = fundamental_dataset.time_series
      FindTimeSeriesWeeklyObservation.at_or_earlier_than(time_series, datestamp)
    end
  end
end

class FindFundamentalMonthlyObservation
  class << self
    def at_or_earlier_than(fundamental_dataset, datestamp)
      time_series = fundamental_dataset.time_series
      FindTimeSeriesMonthlyObservation.at_or_earlier_than(time_series, datestamp)
    end
  end
end

class FindFundamentalQuarterlyObservation
  class << self
    def at_or_earlier_than(fundamental_dataset, datestamp)
      time_series = fundamental_dataset.time_series
      FindTimeSeriesQuarterlyObservation.at_or_earlier_than(time_series, datestamp)
    end
  end
end

class FindFundamentalYearlyObservation
  class << self
    def at_or_earlier_than(fundamental_dataset, datestamp)
      time_series = fundamental_dataset.time_series
      FindTimeSeriesYearlyObservation.at_or_earlier_than(time_series, datestamp)
    end
  end
end

class FindFundamentalObservation
  class << self
    def at_or_earlier_than(fundamental_dataset, datestamp)
      time_series = fundamental_dataset.time_series
      case time_series.update_frequency.label
      when UpdateFrequency::DAILY
        FindTimeSeriesDailyObservation.at_or_earlier_than(time_series, datestamp)
      when UpdateFrequency::WEEKLY
        FindTimeSeriesWeeklyObservation.at_or_earlier_than(time_series, datestamp)
      when UpdateFrequency::MONTHLY
        FindTimeSeriesMonthlyObservation.at_or_earlier_than(time_series, datestamp)
      when UpdateFrequency::QUARTERLY
        FindTimeSeriesQuarterlyObservation.at_or_earlier_than(time_series, datestamp)
      when UpdateFrequency::YEARLY
        FindTimeSeriesYearlyObservation.at_or_earlier_than(time_series, datestamp)
      when UpdateFrequency::IRREGULAR
        raise "FindFundamentalObservation.at_or_earlier_than not implemented for time series with irregularly time-spaced observations."
      end
    end
  end
end
