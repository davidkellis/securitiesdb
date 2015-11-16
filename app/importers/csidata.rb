require 'date'
require 'pp'

require_relative "../clients/csidata"

class CsiDataImporter
  CSI_EXCHANGE_TO_EXCHANGE_LABEL_MAP = {
    "AMEX" => ["UA"],
    "NYSE" => ["UN"],
    "OTC" => ["UW", "UQ", "UR", "UV"],                                # prefer nasdaq exchanges first (Global Select, Global Market, SmallCap Market), then OTC
    "MUTUAL" => ["UN", "UA", "UW", "UQ", "UR", "UV", "MUTUAL"],       # put CSI mutual funds into Amex, Nyse, Nasdaq, OTC, or user-defined MUTUAL exchange
    "INDEX" => ["UN", "UA", "UW", "UQ", "UR", "CBOE", "UV", "INDEX"]  # put CSI indices into Amex, Nyse, Nasdaq, CBOE, OTC, or user-defined INDEX exchange
  }

  attr_accessor :csi_client

  def initialize
    self.csi_client = CsiData::Client.new
  end

  def log(msg)
    Application.logger.info(msg)
  end

  def import
    import_amex
    import_nyse
    import_nasdaq
    import_etfs
    import_etns
    import_mutual_funds
    import_us_stock_indices

    import_securities(csi_client.stocks, "Stock")
    import_securities(csi_client.etps, "ETP")
    import_securities(csi_client.mutual_funds, "Fund")
    import_securities(csi_client.us_stock_indices, "Index")
  end

  def import_amex
    log "Importing CSI Data symbols for AMEX."
    us_composite_exchange = Exchange.us_composite
    amex_exchange = Exchange.amex
    csi_securities = csi_client.amex
    csi_securities.each do |csi_security|
    end
  end

  def import_us_stock_indices
    log "Importing CSI Data symbols for US Stock Indices."
    us_composite_exchange = Exchange.us_composite
    us_stock_indices = Exchange.us_stock_indices
    csi_securities = csi_client.amex
    csi_securities.each do |csi_security|
    end
  end

  def lookup_exchanges(csi_exchange_label)
    @exchange_memo[label] ||= Exchange.all(label: label)
  end

  # expected_constituent_exchanges is an array of constituent exchanges that make up, either in part or in whole, the given composite_exchange
  # default_exchange is not a composite exchange
  def import_security(csi_security, composite_exchange, expected_constituent_exchanges, default_exchange)
    exchanges = ([composite_exchange] + expected_constituent_exchanges + [default_exchange]).flatten.compact

    # 2. search for security in any of the exchanges found in (1)
    securities = Security.where(exchange_id: exchanges.map(&:id))

    case securities.count
    when 0        # if no securities found, create the security in default_exchange
      create_security(csi_security)
    when 1        # if one security found, update it
    else # > 1    # if multiple securities found:
      #   if security is in composite exchange, update the composite security
      #   otherwise, security is in constituent exchange
      #     identify the component exchange that is most preferred in the list of <expected_exchanges>, then update
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
  def create_security(csi_security)
    # todo, resume work here
    Security.create(
      csi_number: csi_security.csi_number
      symbol: csi_security.symbol,
      name: csi_security.name,
      start_date: ,
      end_date: ,
      exchange: default_exchange,
      sector: sector,
      industry: industry
    )
  end

  def update_security(security, exchange)
    replacement_attributes = {}
    replacement_attributes[:exchange] = exchange if existing_security.exchange &&
                                                    existing_security.exchange.label != csi_security.pricing_source
    replacement_attributes[:security_type] = security_type if existing_security.security_type &&
                                                              existing_security.security_type.market_sector != csi_security.market_sector &&
                                                              existing_security.security_type.name != csi_security.security_type
    replacement_attributes[:name] = csi_security.name if existing_security.name != csi_security.name
    replacement_attributes[:symbol] = csi_security.ticker if existing_security.symbol != csi_security.ticker
    replacement_attributes[:bbgid_composite] = csi_security.composite_bbgid if existing_security.bbgid_composite != csi_security.composite_bbgid

    existing_security.update(replacement_attributes)
  rescue Sequel::ValidationFailed, Sequel::HookFailed => e
    log "Can't import #{csi_security.inspect}: #{e.message}"
  rescue => e
    log "Can't import #{csi_security.inspect}: #{e.message}"
    log e.backtrace.join("\n")
  end
end
