require 'forwardable'
require 'singleton'

class LookupFundamentals
  include Singleton

  class << self
    extend Forwardable
    def_delegators :instance, :all_observations, :most_recent_observations, :lookup_fundamental_dataset, :lookup_fundamental_observations_dataset
  end


  def all_observations(security, fundamental_attribute_label, fundamental_dimension_name)
    fundamental_dataset = lookup_fundamental_dataset(security, fundamental_attribute_label, fundamental_dimension_name)
    lookup_fundamental_observations_dataset(fundamental_dataset, fundamental_dimension_name)
  end

  def most_recent_observation(security, fundamental_attribute_label, fundamental_dimension_name, date)
    all_observations(security, fundamental_attribute_label, fundamental_dimension_name).
      where {|vr| vr.date <= date }.    # vr means virtual row - see http://sequel.jeremyevans.net/rdoc/files/doc/virtual_rows_rdoc.html
      reverse_order(:date).
      first
  end

  def lookup_fundamental_dataset(security, fundamental_attribute_label, fundamental_dimension_name)
    security.
      fundamental_datasets_dataset.
      select_all(:fundamental_datasets).
      join(:fundamental_attributes, :id => :fundamental_datasets__fundamental_attribute_id).
      join(:fundamental_dimensions, :id => :fundamental_datasets__fundamental_dimension_id).
      where(
        Sequel.qualify(:fundamental_attributes, :label) => fundamental_attribute_label,
        Sequel.qualify(:fundamental_dimensions, :name) => fundamental_dimension_name
      ).
      first
  end

  def lookup_fundamental_observations_dataset(fundamental_dataset, fundamental_dimension_name)
    time_series = fundamental_dataset.time_series
    case fundamental_dimension_name
    when FundamentalDimension::INSTANTANEOUS
      time_series.irregular_observations_dataset
    when FundamentalDimension::ARQ, FundamentalDimension::MRQ, FundamentalDimension::ART_Q, FundamentalDimension::MRT_Q
      time_series.quarterly_observations_dataset
    when FundamentalDimension::ARY, FundamentalDimension::MRY
      time_series.yearly_observations_dataset
    else
      raise "Unknown fundamental dimension name: #{fundamental_dimension_name}"
    end
  end

end
