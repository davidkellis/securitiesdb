class Variable
  def name
    raise "#{self.class.name}#name not implemented."
  end

  # timestamp is an integer timestamp of the form yyyymmddHHMMSS
  def observe(timestamp)
    raise "#{self.class.name}#observe not implemented."
  end

  def memoized(observation_count)
    MemoizedVariable.new(self, observation_count)
  end
end

class MemoizedVariable < Variable
  def initialize(variable, observation_count)
    @variable = variable
    @cache = LruCache.new(observation_count)
  end

  def name
    @variable.name
  end

  def observe(timestamp)
    @cache.get_or_set(timestamp) { @variable.observe(timestamp) }
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

  class AdjustedEodBarClose < Variable
    def initialize(security)
      @security = security
    end

    def name
      "Adjusted EOD Close #{@security.name} (id=#{@security.id})"
    end

    def observe(timestamp)
      datestamp = Date.timestamp_to_datestamp(timestamp)
      tsmap = EodBarLoader.get(@security, datestamp)
      eod_bar = tsmap.latest_value_at_or_earlier_than(datestamp)
      eod_bar.close.to_f if eod_bar
    end
  end

  class DailyTimeSeriesObservation < Variable
    # time_series is a TimeSeries
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

  class LookbackDifference < Variable
    # previous_time_fn is a function (currentTime) -> previousTime
    def initialize(variable, variable_name_prefix, previous_time_fn)
      @variable = variable
      @variable_name_prefix = variable_name_prefix
      @previous_time_fn = previous_time_fn
    end

    def name
      "#{@variable_name_prefix} #{@variable.name}"
    end

    def observe(timestamp)
      current_observation = @variable.observe(timestamp)
      previous_observation = @variable.observe(@previous_time_fn.call(timestamp))
      (current_observation - previous_observation) if current_observation && previous_observation
    end
  end

  class LookbackRatio < Variable
    # previous_time_fn is a function (currentTime) -> previousTime
    def initialize(variable, variable_name_prefix, previous_time_fn)
      @variable = variable
      @variable_name_prefix = variable_name_prefix
      @previous_time_fn = previous_time_fn
    end

    def name
      "#{@variable_name_prefix} #{@variable.name}"
    end

    def observe(timestamp)
      current_observation = @variable.observe(timestamp)
      previous_observation = @variable.observe(@previous_time_fn.call(timestamp))
      (current_observation / previous_observation) if current_observation && previous_observation && previous_observation != 0
    end
  end

  class LookaheadRatio < Variable
    # future_time_fn is a function (currentTime) -> futureTime
    def initialize(variable, variable_name_prefix, future_time_fn)
      @variable = variable
      @variable_name_prefix = variable_name_prefix
      @future_time_fn = future_time_fn
    end

    def name
      "#{@variable_name_prefix} #{@variable.name}"
    end

    def observe(timestamp)
      current_observation = @variable.observe(timestamp)
      if current_observation && current_observation != 0
        future_observation = @variable.observe(@future_time_fn.call(timestamp))
        (future_observation / current_observation) if future_observation
      end
    end
  end

  class PercentileLookbackRatio < Variable
    # time_decrementer_fn is a function (time) -> futureTime
    def initialize(variable, variable_name_prefix, percentile, past_observation_count, time_decrementer_fn)
      @variable = variable
      @variable_name_prefix = variable_name_prefix
      @percentile = percentile
      @past_observation_count = past_observation_count
      @time_decrementer_fn = time_decrementer_fn
    end

    def name
      "#{@variable_name_prefix} #{@variable.name}"
    end

    def observe(timestamp)
      current_observation = @variable.observe(timestamp)
      if current_observation
        prev_timestamp = timestamp
        return_observations = @past_observation_count.times.map do
          prev_timestamp = @time_decrementer_fn.call(prev_timestamp)
          past_observation = @variable.observe(prev_timestamp)
          (current_observation / past_observation) if past_observation && past_observation != 0
        end.compact
        Stats.percentiles([@percentile], return_observations).first unless return_observations.empty?
      end
    end
  end

  class PercentileLookaheadRatio < Variable
    # time_incrementer_fn is a function (time) -> futureTime
    def initialize(variable, variable_name_prefix, percentile, future_observation_count, time_incrementer_fn)
      @variable = variable
      @variable_name_prefix = variable_name_prefix
      @percentile = percentile
      @future_observation_count = future_observation_count
      @time_incrementer_fn = time_incrementer_fn
    end

    def name
      "#{@variable_name_prefix} #{@variable.name}"
    end

    def observe(timestamp)
      current_observation = @variable.observe(timestamp)
      if current_observation && current_observation != 0
        next_timestamp = timestamp
        future_return_observations = @future_observation_count.times.map do
          next_timestamp = @time_incrementer_fn.call(next_timestamp)
          future_observation = @variable.observe(next_timestamp)
          (future_observation / current_observation) if future_observation
        end.compact
        Stats.percentiles([@percentile], future_return_observations).first unless future_return_observations.empty?
      end
    end
  end
end
