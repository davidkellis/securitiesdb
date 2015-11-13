require 'date'
require 'pp'

require_relative "../clients/csidata"

class CsiDataImporter
  attr_accessor :csi_client

  def initialize
    self.csi_client = CsiData::Client.new
  end

  def log(msg)
    Application.logger.info(msg)
  end

  def import
    import_securities(csi_client.stocks, "Stock")
    import_securities(csi_client.etps, "ETP")
    import_securities(csi_client.funds, "Fund")
    import_securities(csi_client.indices, "Index")
  end

  def import_securities(csi_securities, asset_class_category)
    log "Importing CSI Data symbols for asset class category #{asset_class_category}."
    csi_securities.each {|csi_security| import_security(csi_security) }
  end

  def import_security(csi_security)
    create_or_update_security(csi_security)
  end

  def lookup_exchange(label)
    @exchange_memo[label] ||= Exchange.first(label: label)
  end

  def lookup_security_type(market_sector, security_type)
    @security_type_memo[label] ||= SecurityType.first(market_sector: market_sector, name: security_type)
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
  def create_or_update_security(csi_security)
    # todo: resume here
  #   exchange = lookup_exchange(csi_security.pricing_source)
  #   security_type = lookup_security_type(csi_security.market_sector, csi_security.security_type)
  #   if exchange && security_type
  #     existing_security = Security.first(figi: csi_security.figi)
  #     if existing_security
  #       replacement_attributes = {}
  #       replacement_attributes[:exchange] = exchange if existing_security.exchange &&
  #                                                       existing_security.exchange.label != csi_security.pricing_source
  #       replacement_attributes[:security_type] = security_type if existing_security.security_type &&
  #                                                                 existing_security.security_type.market_sector != csi_security.market_sector &&
  #                                                                 existing_security.security_type.name != csi_security.security_type
  #       replacement_attributes[:name] = csi_security.name if existing_security.name != csi_security.name
  #       replacement_attributes[:symbol] = csi_security.ticker if existing_security.symbol != csi_security.ticker
  #       replacement_attributes[:bbgcid] = csi_security.composite_bbgid if existing_security.bbgcid != csi_security.composite_bbgid
  #
  #       existing_security.update(replacement_attributes)
  #     else
  #       Security.create(
  #         figi: figi,
  #         bb_gcid: bb_gcid,
  #         name: name,
  #         symbol: symbol,
  #         exchange: exchange ? exchange : []
  #       )
  #     end
  #   else
  #     log "Unknown exchange, #{csi_security.pricing_source.inspect}, or unknown security type, (#{csi_security.market_sector.inspect}, #{csi_security.security_type.inspect}). Security defined as #{csi_security.inspect}"
  #   end
  # rescue Sequel::ValidationFailed, Sequel::HookFailed => e
  #   log "Can't import #{csi_security.inspect}: #{e.message}"
  # rescue => e
  #   log "Can't import #{csi_security.inspect}: #{e.message}"
  #   log e.backtrace.join("\n")
  end
end
