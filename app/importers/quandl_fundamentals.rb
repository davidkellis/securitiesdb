require 'csv'
require 'zip'

class QuandlFundamentalsImporter
  def initialize
    @us_stock_exchanges = Exchange.us_stock_exchanges.to_a
    @us_composite = Exchange.us_composite
  end

  def import
    all_fundamentals = QuandlFundamentals::Client.new.all_fundamentals
    import_fundamentals(all_fundamentals)
  end

  private

  def log(msg)
    Application.logger.info("#{Time.now} - #{msg}")
  end

  def import_fundamentals(all_fundamentals)
    all_fundamentals.each do |symbol, attribute_datasets|
      # attribute_datasets is a Hash of the form: { attributeName1 => AttributeDataset1, attributeName2 => AttributeDataset2, ... }
      # where each AttributeDataset is a struct of the form:
      # AttributeDataset = Struct.new(
      #   :ticker,
      #   :attribute,
      #   :attribute_values
      # )
      # AttributeValue = Struct.new(:date, :value)
      if !attribute_datasets.empty? && !attribute_datasets.first[1].attribute_values.empty?
        date_of_first_attribute_value = attribute_datasets.first[1].attribute_values.first.date
        security = lookup_security(symbol, date_of_first_attribute_value)
        if security
          attribute_datasets.each do |attribute_name, attribute_dataset|
            most_recent_attribute_value = security.
                                            fundamental_data_points_dataset.
                                            where(Sequel.qualify(:fundamental_attributes, :name) => attribute_name).
                                            reverse_order(:start_date).
                                            first

            if most_recent_attribute_value
              import_missing_fundamentals(
                security,
                attribute_name,
                attribute_dataset.attribute_values.select {|attribute_value| attribute_value.date > most_recent_attribute_value.start_date }
              )
            else
              import_missing_fundamentals(security, attribute_name, attribute_dataset.attribute_values)
            end
          end
        else
          log "Security symbol '#{symbol}' not found in any US exchange."
        end
    end
  end

  # search for the security (1) in the appropriate composite exchange, (2) in the appropriate constituent (local) exchanges, and finally (3) in the catch-all exchange
  def lookup_security(symbol, date)
    # 1. search for the security in the appropriate composite exchange
    begin
      Security.first(symbol: symbol, exchange_id: @us_composite.id)
    end ||
    # 2. if not found in (1), search in the appropriate constituent (local) exchange(s)
    begin
      securities_in_local_exchanges = Security.where(symbol: symbol, exchange_id: @us_stock_exchanges.map(&:id)).to_a
      case securities_in_local_exchanges.count
      when 0
        nil
      when 1
        securities_in_local_exchanges.first
      else
        # todo: figure out which exchange is preferred, and then return the security in the most preferred exchange
        raise "Symbol #{symbol} is listed in multiple exchanges: #{securities_in_local_exchanges.map(&:exchange).map(&:label)}"
      end
    end ||
    # 3. if not found in (1) or (2), search in the appropriate catch-all exchange(s)
    begin
      securities_in_catch_all_exchanges = Security.
                                            where(symbol: symbol, exchange_id: Exchange.catch_all_stock.id).
                                            where { (start_date <= date) & (end_date >= date) }.
                                            to_a
      case securities_in_catch_all_exchanges.count
      when 0
        nil
      when 1
        securities_in_catch_all_exchanges.first
      else
        raise "Multiple securities in the catch all exchange(s) traded under the symbol '#{symbol}' on #{date}: #{securities_in_catch_all_exchanges.inspect}"
      end
    end
  end

  # attribute_values is an array of QuandlFundamentals::AttributeValue objects
  def import_missing_fundamentals(security, attribute_name, attribute_values)
    log "Importing #{attribute_values.count} missing values of attribute '#{attribute_name}' from Quandl Fundamentals database for symbol #{symbol}."

    attribute = find_or_create_fundamental_attribute(attribute_name)

    attribute_values.each do |attribute_value|
      FundamentalDataPoint.create(
        security_id: security.id,
        fundamental_attribute_id: attribute.id,
        value: attribute_value.value,
        start_date: attribute_value.date
      )
    end
  end

  def find_or_create_fundamental_attribute(attribute_name)
    FundamentalAttribute.first(name: attribute_name) || FundamentalAttribute.create(name: attribute_name)
  end

end
