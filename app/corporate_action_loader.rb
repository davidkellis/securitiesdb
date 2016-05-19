require 'singleton'

class CorporateActionLoader < TimeSeriesMapLoader
  include Singleton

  def self.get(security)
    instance.get(security)
  end


  def initialize()
    super(100)  # 100 TimeSeriesMap objects - one per security
  end

  protected

  # compute cache key that identifies a unique Security
  def cache_key(security)
    security.id
  end

  # query the database all the corporate actions associated with <security>
  def find_observations(security)
    security.corporate_actions.to_a
  end

  def extract_observation_time(corporate_action)
    corporate_action.ex_date
  end

  def extract_observation_value(corporate_action)
    corporate_action
  end
end
