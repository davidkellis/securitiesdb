# This is all deprecated since http://bsym.bloomberg.com/sym/ is shutting down in favor of the very much *closed* https://www.openfigi.com/
#
# require 'date'
# require 'pp'
#
# require_relative "../clients/bsym"
#
# class BsymSecuritiesImporter
#   attr_accessor :bsym_client
#
#   def initialize
#     self.bsym_client = Bsym::Client.new(Application.logger)
#
#     @exchange_memo = {}
#     @security_type_memo = {}
#   end
#
#   def log(msg)
#     Application.logger.info("#{Time.now} - #{msg}")
#   end
#
#   def import(exchanges_to_import = Exchange.us_exchanges)
#     exchange_labels = exchanges_to_import.map(&:label)
#     selection_predicate = ->(bsym_security) { exchange_labels.include?(bsym_security.pricing_source) }
#
#     import_securities(bsym_client.stocks.select(&selection_predicate), "Stock")
#     import_securities(bsym_client.etps.select(&selection_predicate), "ETP")
#     import_securities(bsym_client.funds.select(&selection_predicate), "Fund")
#     import_securities(bsym_client.indices.select(&selection_predicate), "Index")
#
#     # import_custom_securities
#   end
#
#   def import_securities(bsym_securities, asset_class_category)
#     log "Importing #{bsym_securities.count} Bloomberg symbols for asset class category #{asset_class_category}."
#     bsym_securities.each {|bsym_security| import_security(bsym_security) }
#   end
#
#   def import_security(bsym_security)
#     create_or_update_security(bsym_security)
#   end
#
#   def lookup_exchange(label)
#     @exchange_memo[label] ||= Exchange.first(label: label)
#   end
#
#   def find_or_create_security_type(market_sector, security_type)
#     @security_type_memo["#{market_sector}-#{security_type}"] ||= begin
#       SecurityType.first(market_sector: market_sector, name: security_type) || SecurityType.create(market_sector: market_sector, name: security_type)
#     end
#   end
#
#   # def import_custom_securities
#   #   log "Importing user-defined securities."
#   #   create_or_update_security("CBOE", "BBGDKE1", "BBGDKE1", "CBOE 1 Month SPX Volatility Index", "^VIX")
#   #   create_or_update_security("CBOE", "BBGDKE2", "BBGDKE2", "CBOE 3 Month SPX Volatility Index", "^VXV")
#   # end
#
#   # Bsym::Security is defined as Security = Struct.new(:name, :ticker, :pricing_source, :bsid, :unique_id, :security_type, :market_sector, :figi, :composite_bbgid)
#   def create_or_update_security(bsym_security)
#     exchange = lookup_exchange(bsym_security.pricing_source)
#     security_type = find_or_create_security_type(bsym_security.market_sector, bsym_security.security_type)
#     if exchange && security_type
#       existing_security = Security.first(figi: bsym_security.figi)
#       if existing_security
#         replacement_attributes = {}
#         replacement_attributes[:exchange] = exchange if existing_security.exchange &&
#                                                         existing_security.exchange.label != bsym_security.pricing_source
#         replacement_attributes[:security_type] = security_type if existing_security.security_type &&
#                                                                   existing_security.security_type.market_sector != bsym_security.market_sector &&
#                                                                   existing_security.security_type.name != bsym_security.security_type
#         replacement_attributes[:name] = bsym_security.name if existing_security.name != bsym_security.name
#         replacement_attributes[:symbol] = bsym_security.ticker if existing_security.symbol != bsym_security.ticker
#         replacement_attributes[:bbgid_composite] = bsym_security.composite_bbgid if existing_security.bbgid_composite != bsym_security.composite_bbgid
#
#         existing_security.update(replacement_attributes)
#       else
#         Security.create(
#           figi: bsym_security.figi,
#           bbgid_composite: bsym_security.composite_bbgid,
#           name: bsym_security.name,
#           symbol: bsym_security.ticker,
#           exchange: exchange,
#           security_type: security_type
#         )
#       end
#     else
#       log "Unknown exchange, #{bsym_security.pricing_source.inspect}, or unknown security type, (#{bsym_security.market_sector.inspect}, #{bsym_security.security_type.inspect}). Security defined as #{bsym_security.inspect}"
#     end
#   rescue Sequel::ValidationFailed, Sequel::HookFailed => e
#     log "Can't import #{bsym_security.inspect}: #{e.message}"
#   rescue => e
#     log "Can't import #{bsym_security.inspect}: #{e.message}"
#     log e.backtrace.join("\n")
#   end
# end
