require 'treemap'

class TimeSeriesMap
  def initialize(navigable_map = nil)
    @navigable_map = navigable_map || TreeMap.new
  end

  def add(time, value)
    @navigable_map.put(time, value)
  end

  def remove(time)
    @navigable_map.remove(time)
  end

  # returns array of keys
  def keys
    @navigable_map.key_set.to_a
  end

  # returns array of values
  def values
    @navigable_map.values.to_a
  end

  def [](time)
    @navigable_map[time]
  end

  def get(time)
    @navigable_map[time]
  end

  def between(start_time, end_time, inclusive_of_start_time = true, inclusive_of_end_time = false)
    TimeSeriesMap.new(@navigable_map.sub_map(start_time, inclusive_of_start_time, end_time, inclusive_of_end_time))
  end

  def latest_value_at_or_earlier_than(time)
    entry = @navigable_map.floor_entry(time)    # floorKey returns the greatest key less than or equal to the given key, or null if there is no such key.
    entry.value if entry
  end

  def latest_value_earlier_than(time)
    key = @navigable_map.lower_entry(time)    # lowerKey returns the greatest key strictly less than the given key, or null if there is no such key.
    entry.value if entry
  end

  def earliest_value_at_or_later_than(time)
    key = @navigable_map.ceiling_entry(time)  # ceilingKey returns the least key greater than or equal to the given key, or null if there is no such key.
    entry.value if entry
  end

  def earliest_value_later_than(time)
    key = @navigable_map.higher_entry(time)   # higherKey returns the least key strictly greater than the given key, or null if there is no such key.
    entry.value if entry
  end
end
