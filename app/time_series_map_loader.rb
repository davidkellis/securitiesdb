class TimeSeriesMapLoader
  attr_accessor :cache

  def initialize(cache_size = 100)
    self.cache = LruCache.new(cache_size)
  end

  def get(aggregate_entity, time)
    key = cache_key(aggregate_entity, time)
    cache.get(key) || load_observations_into_cache(aggregate_entity, time, key)
  end

  protected

  # compute cache key that will identify the subset of observations belonging to <aggregate_entity> that were relevant at time <time>
  def cache_key(aggregate_entity, time)
    raise "TimeSeriesMapLoader#cache_key not implemented."
  end

  # query the database for a subset of observations belonging to <aggregate_entity> that were relevant at time <time>
  def find_observations(aggregate_entity, time)
    raise "TimeSeriesMapLoader#find_observations not implemented."
  end

  def extract_observation_time(observation)
    raise "TimeSeriesMapLoader#extract_observation_time not implemented."
  end

  def extract_observation_value(observation)
    observation
  end

  private

  def load_observations_into_cache(aggregate_entity, time, precomputed_cache_key)
    time_series_map = load_observations_into_time_series_map(aggregate_entity, time)
    cache.set(precomputed_cache_key, time_series_map) if time_series_map
    time_series_map
  end

  # returns a TimeSeriesMap[time, observation] storing a subset of the observations belonging to <aggregate_entity>
  def load_observations_into_time_series_map(aggregate_entity, time)
    map = TimeSeriesMap.new
    observations = find_observations(aggregate_entity, time)
    observations.each {|observation| map.add(extract_observation_time(observation), extract_observation_value(observation)) }
    map
  end
end
