require 'csv'
require 'zip'

class QuandlFundamentalsImporter
  def initialize(quandl_fundamentals_client)
    @lookup_security = LookupSecurity.new(Exchange.us_composite, Exchange.us_stock_exchanges.to_a, Exchange.catch_all_stock)
    @client = quandl_fundamentals_client
  end

  def import
    indicators = @client.indicators
    import_indicators(indicators)

    all_fundamentals = @client.all_fundamentals
    import_fundamentals(all_fundamentals)
  end

  private

  def log(msg)
    Application.logger.info("#{Time.now} - #{msg}")
  end

  # indicators is an array of QuandlFundamentals::Indicator objects
  def import_indicators(indicators)
    indicators.map do |indicator|
      FundamentalAttribute.create(
        label: indicator.label,
        name: indicator.title,
        description: indicator.description
      ) unless lookup_fundamental_attribute(indicator.label)
    end
  end

  def import_fundamentals(all_fundamentals)
    all_fundamentals.each do |ticker, indicator, dimension, indicator_values|
      # IndicatorValue = Struct.new(:date, :value)
      dimension ||= FundamentalDimension::INSTANTANEOUS
      if !indicator_values.empty?
        date_of_first_attribute_value = indicator_values.first.date
        security = @lookup_security.run(ticker, date_of_first_attribute_value)
        if security
          most_recent_attribute_value = security.
                                          fundamental_data_points_dataset.
                                          join(:fundamental_attributes, :id => :fundamental_attribute_id).
                                          join(:fundamental_dimensions, :id => :fundamental_data_points__fundamental_dimension_id).
                                          where(
                                            Sequel.qualify(:fundamental_attributes, :label) => indicator,
                                            Sequel.qualify(:fundamental_dimensions, :name) => dimension
                                          ).
                                          reverse_order(:start_date).
                                          first
          if most_recent_attribute_value
            import_missing_fundamentals(
              security,
              indicator,
              dimension,
              indicator_values.select {|indicator_value| indicator_value.date > most_recent_attribute_value.start_date }
            )
          else
            import_missing_fundamentals(security, indicator, dimension, indicator_values)
          end
        else
          log "Security symbol '#{ticker}' not found in any US exchange."
        end
      end
    end
  end

  # indicator_values is an array of QuandlFundamentals::IndicatorValue objects
  def import_missing_fundamentals(security, attribute_label, dimension_name, indicator_values)
    log "Importing #{indicator_values.count} missing values of attribute '#{attribute_label}' (dimension=#{dimension_name}) from Quandl Fundamentals database for symbol #{security.symbol} (security id=#{security.id})."

    attribute = lookup_fundamental_attribute(attribute_label) || raise("Unknown fundamental attribute: #{attribute_label}.")
    dimension = lookup_fundamental_dimension(dimension_name) || raise("Unknown fundamental dimension: #{dimension_name}.")

    indicator_values.each do |indicator_value|
      FundamentalDataPoint.create(
        security_id: security.id,
        fundamental_attribute_id: attribute.id,
        fundamental_dimension_id: dimension.id,
        start_date: indicator_value.date,
        value: indicator_value.value
      )
    end
  end

  def lookup_fundamental_attribute(attribute_label)
    FundamentalAttribute.first(label: attribute_label)
  end

  def lookup_fundamental_dimension(dimension_name)
    FundamentalDimension.first(name: dimension_name)
  end

end
