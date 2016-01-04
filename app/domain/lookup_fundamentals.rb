class LookupFundamentals
  def self.all(security, attribute_name, dimension_name)
    security.
      fundamental_data_points_dataset.
      join(:fundamental_attributes, :id => :fundamental_attribute_id).
      join(:fundamental_dimensions, :id => :fundamental_data_points__fundamental_dimension_id).
      where(
        Sequel.qualify(:fundamental_attributes, :label) => attribute_name,
        Sequel.qualify(:fundamental_dimensions, :name) => dimension_name
      ).
      reverse_order(:start_date)
  end

  def self.most_recent(security, attribute_name, dimension_name, date)
    security.
      fundamental_data_points_dataset.
      join(:fundamental_attributes, :id => :fundamental_attribute_id).
      join(:fundamental_dimensions, :id => :fundamental_data_points__fundamental_dimension_id).
      where(
        Sequel.qualify(:fundamental_attributes, :label) => attribute_name,
        Sequel.qualify(:fundamental_dimensions, :name) => dimension_name
      ).
      where { start_date <= date }.
      reverse_order(:start_date).
      first
  end
end
