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
