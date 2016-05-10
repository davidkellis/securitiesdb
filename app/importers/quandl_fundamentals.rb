require 'csv'
require 'zip'

class QuandlFundamentalsImporter
  APPROXIMATE_SEARCH_THRESHOLD = 0.7
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
    @similarity_measure = SimString::ComputeSimilarity.new(SimString::NGramBuilder.new(3), SimString::CosineMeasure.new)
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

  def error(msg)
    Application.logger.error("#{Time.now} - #{msg}")
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
    ticker_to_security = @client.securities.map {|s| [s.ticker, s] }.to_h
    log "Importing fundamentals."
    all_fundamentals.each do |ticker, indicator, quandl_dimension, indicator_values|
      # IndicatorValue = Struct.new(:date, :value)
      quandl_dimension ||= FundamentalDimension::INSTANTANEOUS
      fundamental_dimension_name = DIMENSION_TRANSLATION_TABLE[quandl_dimension]
      if !indicator_values.empty?
        date_of_first_attribute_value = indicator_values.first.date               # an integer value
        securities = @find_security.all(ticker, date_of_first_attribute_value)    # try to identify the security that was actively trading under the ticker at that date
        case securities.count
        when 0
          log "Security symbol '#{ticker}' not found in any US exchange."
        when 1
          security = securities.first
          import_fundamentals_for_single_security(security, ticker, indicator, fundamental_dimension_name, indicator_values)
        else
          security_reference = ticker_to_security[ticker]   # this is a QuandlFundamentals::Security object
          if security_reference
            db = SecurityNameDatabase.new
            security_name_to_security = securities.map {|security| [security.name.downcase, security] }.to_h
            securities.each {|security| db.add(security.name.downcase) }
            matches = db.ranked_search(security_reference.name.downcase, APPROXIMATE_SEARCH_THRESHOLD)
            case matches.count
            when 0
              security_references = securities.map do |security|
                "#{security.name} - #{@similarity_measure.similarity(security.name.downcase, security_reference.name)}"
              end
              error "Error: Security symbol '#{ticker}' identifies multiple securities but none match the Quandl SF1 security name '#{security_reference.name}':\n#{security_references.join("\n")}."
            when 1
              match = matches.first
              matching_security = security_name_to_security[match.value]
              import_fundamentals_for_single_security(matching_security, ticker, indicator, fundamental_dimension_name, indicator_values)
            else
              security_references = matches.map {|match| "#{match.value} - #{match.score}" }
              error "Error: Security symbol '#{ticker}' identifies multiple matching securities. The following securities approximately match '#{security_reference.name}':\n#{security_references.join("\n")}."
            end
          else
            security_references = securities.map(&:to_hash)
            error "Error: Security symbol '#{ticker}' identifies multiple securities:\n#{security_references.join("\n")}."
          end
        end
      end
    end
  end

  def import_fundamentals_for_single_security(security, ticker, indicator, fundamental_dimension_name, indicator_values)
    fundamental_dataset = LookupFundamentals.lookup_fundamental_dataset(security, indicator, fundamental_dimension_name) ||
                            create_fundamental_dataset(security, ticker, indicator, fundamental_dimension_name)
    most_recent_attribute_value = LookupFundamentals.lookup_fundamental_observations_dataset(fundamental_dataset, fundamental_dimension_name).
                                    reverse_order(:date).
                                    first
    if most_recent_attribute_value
      missing_indicator_values = indicator_values.select {|indicator_value| indicator_value.date > most_recent_attribute_value.date }
      log("Importing #{missing_indicator_values.count} missing observations of indicator #{indicator} (#{fundamental_dimension_name})")
      import_missing_fundamentals(
        fundamental_dataset,
        missing_indicator_values
      )
    else
      log("Importing #{indicator_values.count} observations of indicator #{indicator} (#{fundamental_dimension_name})")
      import_missing_fundamentals(fundamental_dataset, indicator_values)
    end
  end

  def create_fundamental_dataset(security, ticker, fundamental_attribute_label, fundamental_dimension_name)
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
    quandl_dataset_name = "#{ticker}_#{fundamental_attribute_label}_#{fundamental_dimension_name}"

    time_series = lookup_time_series(@data_vendor, QUANDL_CORE_US_FUNDAMENTALS_DATABASE, quandl_dataset_name) ||
                  create_time_series(@data_vendor, update_frequency, QUANDL_CORE_US_FUNDAMENTALS_DATABASE, quandl_dataset_name)

    FundamentalDataset.create(
      security_id: security.id,
      fundamental_attribute_id: fundamental_attribute.id,
      fundamental_dimension_id: fundamental_dimension.id,
      time_series_id: time_series.id
    )
  end

  def lookup_time_series(data_vendor, database, dataset)
    TimeSeries.first(
      data_vendor_id: data_vendor.id,
      database: database,
      dataset: dataset
    )
  end

  def create_time_series(data_vendor, update_frequency, database, dataset)
    TimeSeries.create(
      data_vendor_id: data_vendor.id,
      update_frequency_id: update_frequency.id,
      database: database,
      dataset: dataset
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
