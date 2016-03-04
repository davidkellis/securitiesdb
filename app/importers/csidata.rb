require 'date'
require 'pp'

require_relative "../clients/csidata"

class CsiDataImporter
  UNKNOWN_INDUSTRY_NAME = "UNKNOWN"
  UNKNOWN_SECTOR_NAME = "UNKNOWN"

  # this is a mapping from [CSI Exchange, CSI Child Exchange] to exchange label as defined in ExchangesImporter
  CSI_EXCHANGE_PAIR_TO_EXCHANGE_LABEL_MAP = {
    ["NYSE", nil] => "NYSE",
    ["AMEX", nil] => "NYSE-MKT",
    ["NYSE", "NYSE"] => "NYSE",
    ["OTC", nil] => "OTC",
    ["AMEX", "AMEX"] => "NYSE-MKT",
    ["OTC", "OTC Markets Pink Sheets"] => "OTC-PINK",
    ["OTC", "OTC Markets QX"] => "OTC-QX",
    ["OTC", "Nasdaq Global Select"] => "NASDAQ-GSM",
    ["OTC", "Nasdaq Global Market"] => "NASDAQ-GM",
    ["OTC", "OTC Markets QB"] => "OTC-QB",
    ["OTC", "Nasdaq Capital Market"] => "NASDAQ-CM",
    ["MUTUAL", "Mutual Fund"] => "MUTUAL",
    ["MUTUAL", nil] => "MUTUAL",
    ["AMEX", "NYSE ARCA"] => "NYSE-ARCA",
    ["INDEX", nil] => "INDEX",
    ["AMEX", "NYSE"] => "NYSE",
    ["INDEX", "Stock Indices"] => "INDEX",
    ["FINDEX", "Foreign Stock Indices"] => "INDEX",
    ["FINDEX", nil] => "INDEX",
    ["OTC", "NYSE"] => "NYSE",
    ["NYSE", "AMEX"] => "NYSE-MKT",
    ["NYSE", "OTC Markets Pink Sheets"] => "OTC-PINK",
    # ["TSX", "Toronto Stock Exchange"] => "???",
    # ["VSE", "TSX Venture Exchange"] => "???",
    # ["MSE", "Montreal Stock Exchange"] => "???",
    ["OTC", "AMEX"] => "NYSE-MKT",
    ["AMEX", "OTC Markets QX"] => "OTC-QX",
    ["NYSE", "NYSE ARCA"] => "NYSE-ARCA",
    ["NYSE", "BATS Global Markets"] => "BATS-CATCHALL",
    # ["LSE", "London Stock Exchange"] => "???",
    # ["ALBRTA", "Alberta Stock Exchange"] => "???",
    # ["OTC", "Grey Market"] => "",
    ["NYSE", "Nasdaq Capital Market"] => "NASDAQ-CM",
    ["AMEX", "OTC Markets QB"] => "OTC-QB",
    ["NYSE", "OTC Markets QX"] => "OTC-QX",
    # ["VSE", "Toronto Stock Exchange"] => "",
    ["OTC", "NYSE ARCA"] => "NYSE-ARCA",
    ["AMEX", "OTC Markets Pink Sheets"] => "OTC-PINK",
    ["NYSE", "OTC Markets QB"] => "OTC-QB",
    ["NYSE", "Nasdaq Global Market"] => "NASDAQ-GM",
    ["AMEX", "BATS Global Markets"] => "BATS-CATCHALL",
    ["OTC", "BATS Global Markets"] => "BATS-CATCHALL",
    # ["TSX", nil] => "",
    ["NYSE", "Nasdaq Global Select"] => "NASDAQ-GSM"
    # ["LSE", nil] => ""
  }

  attr_accessor :csi_client

  def initialize
    self.csi_client = CsiData::Client.new
  end

  def log(msg)
    Application.logger.info("#{Time.now} - #{msg}")
  end

  def import
    import_amex
    import_nyse
    import_nasdaq
    import_etfs
    import_etns
    import_mutual_funds
    import_us_stock_indices
  end

  def import_amex
    log "Importing CSI Data symbols for AMEX."
    csi_securities = csi_client.amex
    default_exchange = Exchange.catch_all_stock
    import_securities(csi_securities, default_exchange)
  end

  def import_nyse
    log "Importing CSI Data symbols for NYSE."
    csi_securities = csi_client.nyse
    default_exchange = Exchange.catch_all_stock
    import_securities(csi_securities, default_exchange)
  end

  def import_nasdaq
    log "Importing CSI Data symbols for Nasdaq + OTC."
    csi_securities = csi_client.nasdaq_otc
    default_exchange = Exchange.catch_all_stock
    import_securities(csi_securities, default_exchange)
  end

  def import_etfs
    log "Importing CSI Data symbols for ETFs."
    csi_securities = csi_client.etfs
    default_exchange = Exchange.catch_all_etp
    import_securities(csi_securities, default_exchange)
  end

  def import_etns
    log "Importing CSI Data symbols for ETNs."
    csi_securities = csi_client.etns
    default_exchange = Exchange.catch_all_etp
    import_securities(csi_securities, default_exchange)
  end

  def import_mutual_funds
    log "Importing CSI Data symbols for Mutual Funds."
    csi_securities = csi_client.mutual_funds
    default_exchange = Exchange.catch_all_mutual
    import_securities(csi_securities, default_exchange)
  end

  def import_us_stock_indices
    log "Importing CSI Data symbols for US Stock Indices."
    csi_securities = csi_client.us_stock_indices
    default_exchange = Exchange.indices
    import_securities(csi_securities, default_exchange)
  end

  def import_securities(csi_securities, default_exchange)
    log("Importing #{csi_securities.count} securities from CSI.")
    csi_securities.each do |csi_security|
      import_security(csi_security, default_exchange)
    end
  end

  def lookup_exchange(csi_security)
    csi_exchange_pair = [csi_security.exchange, csi_security.child_exchange]
    exchange_label = CSI_EXCHANGE_PAIR_TO_EXCHANGE_LABEL_MAP[ csi_exchange_pair ]
    @exchange_memo[exchange_label] ||= Exchange.first(label: exchange_label)
  end

  # default_exchange is not a composite exchange
  def import_security(csi_security, default_exchange)
    exchange = lookup_exchange(csi_security) || default_exchange

    securities = Security.
                   association_join(:listed_securities).
                   where(
                     :listed_securities__exchange_id: exchange.id,
                     :listed_securities__symbol: csi_security.symbol
                   ).to_a

    case securities.count
    when 0                                  # if no securities found, create the security
      log("Creating #{csi_security.symbol} in #{exchange.label}")
      create_security(csi_security, exchange)
    when 1                                  # if one security found, update it
      security = securities.first
      log("Updating #{security.symbol} (id=#{security.id})")
      update_security(security, csi_security)
    else # > 1                              # if multiple securities found:
      composite_security = securities.detect {|security| security.exchange == composite_exchange }
      if composite_security                 #   if security is in composite exchange, update the composite security
        log("Updating composite security #{composite_security.symbol} (id=#{composite_security.id})")
        update_security(composite_security, csi_security)
      else                                  #   otherwise, security is in constituent exchange, so identify the proper security
        # we want to identify the security residing in the component exchange that is most preferred in the list of <expected_exchanges>, then update that security
        constituent_exchange_to_rank = expected_constituent_exchanges.each_with_index.to_h
        get_exchange_rank = ->(security) { constituent_exchange_to_rank[security.exchange] || 1_000_000_000 }
        security_with_most_preferred_constituent_exchange = securities.min_by {|security| get_exchange_rank.(security) }

        log("Updating component security #{security_with_most_preferred_constituent_exchange.symbol} (id=#{security_with_most_preferred_constituent_exchange.id}); #{security_with_most_preferred_constituent_exchange.inspect}")
        update_security(security_with_most_preferred_constituent_exchange, csi_security)
      end
    end
  end

  # CsiData::Security is defined as
  # Struct.new(
  #   :csi_number,
  #   :symbol,
  #   :name,
  #   :exchange,
  #   :is_active,
  #   :start_date,
  #   :end_date,
  #   :sector,
  #   :industry,
  #   :conversion_factor,
  #   :switch_cf_date,
  #   :pre_switch_cf,
  #   :last_volume,
  #   :type,
  #   :child_exchange,
  #   :currency
  # )
  def create_security(csi_security, exchange)
    sector = find_or_create_sector(csi_security.sector)
    industry = find_or_create_industry(csi_security.industry)
    Security.create(
      csi_number: csi_security.csi_number.to_i,
      symbol: csi_security.symbol,
      name: csi_security.name,
      start_date: convert_date(csi_security.start_date),
      end_date: convert_date(csi_security.end_date),
      exchange: exchange,
      sector: sector,
      industry: industry
    )
  end

  # CsiData::Security is defined as
  # Struct.new(
  #   :csi_number,
  #   :symbol,
  #   :name,
  #   :exchange,
  #   :is_active,
  #   :start_date,
  #   :end_date,
  #   :sector,
  #   :industry,
  #   :conversion_factor,
  #   :switch_cf_date,
  #   :pre_switch_cf,
  #   :last_volume,
  #   :type,
  #   :child_exchange,
  #   :currency
  # )
  def update_security(existing_security, csi_security)
    replacement_attributes = {}
    replacement_attributes[:csi_number] = csi_security.csi_number if existing_security.csi_number != csi_security.csi_number
    replacement_attributes[:symbol] = csi_security.symbol if existing_security.symbol != csi_security.symbol
    replacement_attributes[:name] = csi_security.name if existing_security.name != csi_security.name
    replacement_attributes[:start_date] = convert_date(csi_security.start_date) if existing_security.start_date != convert_date(csi_security.start_date)
    replacement_attributes[:end_date] = convert_date(csi_security.end_date) if existing_security.end_date != convert_date(csi_security.end_date)

    sector = find_or_create_sector(csi_security.sector || UNKNOWN_SECTOR_NAME)
    replacement_attributes[:sector_id] = sector.id if existing_security.sector_id != sector.id

    industry = find_or_create_industry(csi_security.industry || UNKNOWN_INDUSTRY_NAME)
    replacement_attributes[:industry_id] = industry.id if existing_security.industry_id != industry.id

    existing_security.update(replacement_attributes) unless replacement_attributes.empty?

    existing_security
  rescue Sequel::ValidationFailed, Sequel::HookFailed => e
    log "Can't import #{csi_security.inspect}: #{e.message}"
  rescue => e
    log "Can't import #{csi_security.inspect}: #{e.message}"
    log e.backtrace.join("\n")
  end


  # csi_date is a string of the form "1993-02-01"
  # returns the integer yyyymmdd representation of the csi_date
  def convert_date(csi_date)
    if csi_date
      csi_date.gsub("-","").to_i unless csi_date.empty?
    end
  end

  def find_or_create_sector(sector_name)
    if sector_name && !sector_name.empty?
      Sector.first(name: sector_name) || Sector.create(name: sector_name)
    end
  end

  def find_or_create_industry(industry_name)
    if industry_name && !industry_name.empty?
      Industry.first(name: industry_name) || Industry.create(name: industry_name)
    end
  end

end
