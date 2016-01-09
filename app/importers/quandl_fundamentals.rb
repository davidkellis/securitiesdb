require 'csv'
require 'zip'

class QuandlFundamentalsImporter
  QUANDL_CORE_US_FUNDAMENTALS_DATABASE = "SF1"

  def initialize(quandl_fundamentals_client)
    @lookup_security = LookupSecurity.us_stocks
    @client = quandl_fundamentals_client
    @vendor = DataVendor.first(name: "Quandl")
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
    log "Importing indicators."
    indicators.map do |indicator|
      FundamentalAttribute.create(
        label: indicator.label,
        name: indicator.title,
        description: indicator.description
      ) unless lookup_fundamental_attribute(indicator.label)
    end
  end

  def import_fundamentals(all_fundamentals)
    log "Importing fundamentals."
    all_fundamentals.each do |ticker, indicator, dimension, indicator_values|
      # IndicatorValue = Struct.new(:date, :value)
      dimension ||= FundamentalDimension::INSTANTANEOUS
      if !indicator_values.empty?
        date_of_first_attribute_value = indicator_values.first.date
        security = @lookup_security.run(ticker, date_of_first_attribute_value)
        if security
          fundamental_dataset = lookup_fundamental_dataset(security, indicator, dimension) || create_fundamental_dataset(security, indicator, dimension)
          most_recent_attribute_value = lookup_fundamental_observations(fundamental_dataset, dimension).
                                          reverse_order(:date).
                                          first
          if most_recent_attribute_value
            import_missing_fundamentals(
              fundamental_dataset,
              indicator_values.select {|indicator_value| indicator_value.date > most_recent_attribute_value.date }
            )
          else
            import_missing_fundamentals(fundamental_dataset, indicator_values)
          end
        else
          log "Security symbol '#{ticker}' not found in any US exchange."
        end
      end
    end
  end

  def lookup_fundamental_dataset(security, fundamental_attribute_label, fundamental_dimension_name)
    security.
      fundamental_datasets_dataset.
      join(:fundamental_attributes, :id => :fundamental_attribute_id).
      join(:fundamental_dimensions, :id => :fundamental_datasets__fundamental_dimension_id).
      where(
        Sequel.qualify(:fundamental_attributes, :label) => fundamental_attribute_label,
        Sequel.qualify(:fundamental_dimensions, :name) => fundamental_dimension_name
      ).
      first
  end

  def create_fundamental_dataset(security, fundamental_attribute_label, fundamental_dimension_name)
    update_frequency = case fundamental_dimension_name
    when INSTANTANEOUS
      UpdateFrequency.irregular
    when ARQ, MRQ
      UpdateFrequency.quarterly
    when ARY, MRY, ART, MRT
      UpdateFrequency.yearly
    else
      raise "Unknown fundamental dimension name: #{fundamental_dimension_name}"
    end
    fundamental_attribute = lookup_fundamental_attribute(fundamental_attribute_label)
    fundamental_dimension = lookup_fundamental_dimension(fundamental_dimension_name)
    quandl_dataset_name = "#{security.symbol}_#{fundamental_attribute_label}_#{fundamental_dimension_name}"
    new_time_series = TimeSeries.create(
      vendor_id: @vendor.id,
      update_frequency_id: update_frequency.id,
      database: QUANDL_CORE_US_FUNDAMENTALS_DATABASE,
      dataset: quandl_dataset_name
    )
    FundamentalDataset.create(
      security_id: security.id,
      fundamental_attribute_id: fundamental_attribute.id,
      fundamental_dimension_id: fundamental_dimension.id,
      time_series_id: new_time_series.id
    )
  end

  # indicator_values is an array of QuandlFundamentals::IndicatorValue objects
  def import_missing_fundamentals(fundamental_dataset, indicator_values)
    # log "Importing #{indicator_values.count} missing values of attribute '#{attribute_label}' (dimension=#{dimension_name}) from Quandl Fundamentals database for symbol #{security.symbol} (security id=#{security.id})."

    time_series = fundamental_dataset.time_series

    observation_class = case fundamental_dataset.fundamental_dimension.name
    when INSTANTANEOUS
      IrregularObservation
    when ARQ, MRQ
      QuarterlyObservation
    when ARY, MRY, ART, MRT
      YearlyObservation
    else
      raise "Unknown fundamental dimension name: #{fundamental_dataset.fundamental_dimension.name}"
    end

    indicator_values.each do |indicator_value|
      observation_class.create(
        time_series_id: time_series.id,
        date: indicator_value.date,
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

  def lookup_fundamental_observations(fundamental_dataset, fundamental_dimension_name)
    time_series = fundamental_dataset.time_series
    case fundamental_dimension_name
    when INSTANTANEOUS
      time_series.irregular_observations
    when ARQ, MRQ
      time_series.quarterly_observations
    when ARY, MRY, ART, MRT
      time_series.yearly_observations
    else
      raise "Unknown fundamental dimension name: #{fundamental_dimension_name}"
    end
  end

end
