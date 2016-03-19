require 'csv'
require 'zip'

class QuandlFundamentalsImporter
  QUANDL_CORE_US_FUNDAMENTALS_DATABASE = "SF1"

  DIMENSION_TRANSLATION_TABLE = {
    "INST" => "INST",
    "ARQ" => "ARQ",
    "ARY" => "ARY",
    "ART" => "ART-Q",
    "MRQ" => "MRQ",
    "MRY" => "MRY",
    "MRT" => "MRT-Q",
  }

  def initialize(quandl_fundamentals_client)
    @find_security = FindSecurity.us_stocks
    @client = quandl_fundamentals_client
    @data_vendor = DataVendor.first(name: "Quandl")
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
    all_fundamentals.each do |ticker, indicator, quandl_dimension, indicator_values|
      # IndicatorValue = Struct.new(:date, :value)
      quandl_dimension ||= FundamentalDimension::INSTANTANEOUS
      fundamental_dimension_name = DIMENSION_TRANSLATION_TABLE[quandl_dimension]
      if !indicator_values.empty?
        date_of_first_attribute_value = indicator_values.first.date
        security = @find_security.run(ticker, date_of_first_attribute_value)    # identify the security that was actively trading under the ticker at that date
        if security
          fundamental_dataset = LookupFundamentals.lookup_fundamental_dataset(security, indicator, fundamental_dimension_name) ||
                                  create_fundamental_dataset(security, indicator, fundamental_dimension_name)
          most_recent_attribute_value = LookupFundamentals.lookup_fundamental_observations_dataset(fundamental_dataset, fundamental_dimension_name).
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

  def create_fundamental_dataset(security, fundamental_attribute_label, fundamental_dimension_name)
    update_frequency = case fundamental_dimension_name
    when FundamentalDimension::INSTANTANEOUS
      UpdateFrequency.irregular
    when FundamentalDimension::ARQ, FundamentalDimension::MRQ, FundamentalDimension::ART_Q, FundamentalDimension::MRT_Q
      UpdateFrequency.quarterly
    when FundamentalDimension::ARY, FundamentalDimension::MRY
      UpdateFrequency.yearly
    else
      raise "Unknown fundamental dimension name: #{fundamental_dimension_name}"
    end
    fundamental_attribute = lookup_fundamental_attribute(fundamental_attribute_label)
    fundamental_dimension = lookup_fundamental_dimension(fundamental_dimension_name)
    quandl_dataset_name = "#{security.symbol}_#{fundamental_attribute_label}_#{fundamental_dimension_name}"

    new_time_series = TimeSeries.create(
      data_vendor_id: @data_vendor.id,
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

    observation_class = case time_series.update_frequency.label
    when UpdateFrequency::IRREGULAR
      IrregularObservation
    when UpdateFrequency::DAILY
      DailyObservation
    when UpdateFrequency::WEEKLY
      WeeklyObservation
    when UpdateFrequency::MONTHLY
      MonthlyObservation
    when UpdateFrequency::QUARTERLY
      QuarterlyObservation
    when UpdateFrequency::YEARLY
      YearlyObservation
    else
      raise "Unknown update frequency: #{time_series.update_frequency.inspect}"
    end

    time_series_id = time_series.id
    indicator_values.each do |indicator_value|
      observation_class.create(
        time_series_id: time_series_id,
        date: indicator_value.date,
        value: indicator_value.value
      )
    end
  end

  def lookup_fundamental_attribute(attribute_label)
    FundamentalAttribute.first(label: attribute_label)
  end

  def lookup_fundamental_dimension(dimension_name)
    FundamentalDimension.lookup(dimension_name)
  end

end
