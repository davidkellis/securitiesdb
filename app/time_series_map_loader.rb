class TimeSeriesMapLoader
  attr_accessor :cache

  def initialize(cache_size = 100)
    self.cache = LruCache.new(cache_size)
  end

  def get(aggregate_entity, *observation_selection_criteria)
    key = cache_key(aggregate_entity, *observation_selection_criteria)
    cache.get(key) || load_observations_into_cache(key, aggregate_entity, observation_selection_criteria)
  end

  protected

  # compute cache key that will identify the subset of observations belonging to <aggregate_entity>
  # that were relevant per the given observation_selection_criteria
  def cache_key(aggregate_entity, *observation_selection_criteria)
    raise "TimeSeriesMapLoader#cache_key not implemented."
  end

  # query the database for a subset of observations belonging to <aggregate_entity> that were relevant at time <time>
  def find_observations(aggregate_entity, *observation_selection_criteria)
    raise "TimeSeriesMapLoader#find_observations not implemented."
  end

  def extract_observation_time(observation)
    raise "TimeSeriesMapLoader#extract_observation_time not implemented."
  end

  def extract_observation_value(observation)
    observation
  end

  private

  def load_observations_into_cache(precomputed_cache_key, aggregate_entity, observation_selection_criteria_array)
    time_series_map = load_observations_into_time_series_map(aggregate_entity, observation_selection_criteria_array)
    cache.set(precomputed_cache_key, time_series_map) if time_series_map
    time_series_map
  end

  # returns a TimeSeriesMap[time, observation] storing a subset of the observations belonging to <aggregate_entity>
  def load_observations_into_time_series_map(aggregate_entity, observation_selection_criteria_array)
    map = TimeSeriesMap.new
    observations = find_observations(aggregate_entity, *observation_selection_criteria_array)
    observations.each {|observation| map.add(extract_observation_time(observation), extract_observation_value(observation)) }
    map
  end
end
