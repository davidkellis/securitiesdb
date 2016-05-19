require 'singleton'

class SecurityClassificationLoader < TimeSeriesMapLoader
  include Singleton

  # a nil value for major, minor, or macro means that parameter is a wildcard (*)
  def self.get(security, major = nil, minor = nil, micro = nil)
    instance.get(security, major, minor, micro)
  end


  def initialize()
    super(100)  # 100 TimeSeriesMap objects - one per (Security, major, minor, micro) tuple
  end

  protected

  # compute cache key that identifies a unique (Security, major, minor, micro) tuple
  def cache_key(security, major, minor, micro)
    "#{security.id}-#{major}-#{minor}-#{micro}"
  end

  # query the database for all the SecurityClassifications that are associated with the given (Security, major, minor, micro) tuple
  def find_observations(security, major, minor, micro)
    query = {}
    query[:major] = major if major
    query[:minor] = minor if minor
    query[:micro] = micro if micro
    security.security_classifications.where(query).to_a
  end

  def extract_observation_time(security_classification)
    security_classification.date
  end

  def extract_observation_value(security_classification)
    security_classification
  end
end
