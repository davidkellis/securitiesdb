class FindSecurityClassification
  class << self
    def at_or_earlier_than(security, major = nil, minor = nil, micro = nil, datestamp)
      security_classification_time_series_map = SecurityClassificationLoader.get(security, major, minor, micro)
      security_classification_time_series_map.latest_value_at_or_earlier_than(datestamp)
    end
  end
end
