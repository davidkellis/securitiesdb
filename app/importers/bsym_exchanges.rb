# This is all deprecated since http://bsym.bloomberg.com/sym/ is shutting down in favor of the very much *closed* https://www.openfigi.com/
#
# require 'date'
# require 'pp'
#
# require_relative "../clients/bsym"
#
# class BsymExchangesImporter
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
#   def import
#     import_exchanges(bsym_client.pricing_sources)
#     flag_composite_exchanges(bsym_client.exchange_codes)
#   end
#
#   def import_exchanges(pricing_sources)
#     log "Importing Bloomberg pricing sources as exchanges."
#     pricing_sources.each {|pricing_source| create_or_update_exchange(pricing_source.description, pricing_source.label) }
#
#     log "Creating user-defined exchanges."
#     create_or_update_exchange("Stocks", "STOCK")
#     create_or_update_exchange("ETPs", "ETP")
#     create_or_update_exchange("Indices", "INDEX")
#     create_or_update_exchange("Mutual Funds", "MUTUAL")
#   end
#
#   def create_or_update_exchange(name, label)
#     existing_exchange = Exchange.first(label: label)
#     begin
#       if existing_exchange
#         existing_exchange.update(name: name, label: label)
#       else
#         Exchange.create(name: name, label: label, is_composite_exchange: false)
#       end
#     rescue => e
#       log "Can't import exchange (name=#{name} label=#{label}): #{e.message}"
#     end
#   end
#
#   # exchange_codes is an array of Bsym::ExchangeCode(:composite_exchange_code, :composite_exchange_name, :local_exchange_code, :local_exchange_name)
#   def flag_composite_exchanges(exchange_codes)
#     exchange_codes_by_composite_exchange_code = exchange_codes.group_by(&:composite_exchange_code)
#     exchange_codes_by_composite_exchange_code.each do |composite_exchange_label, exchange_code_list|
#       composite_exchange = Exchange.first(label: composite_exchange_label)
#       composite_exchange_id = composite_exchange.id if composite_exchange
#       constituent_exchange_labels = exchange_code_list.map(&:local_exchange_code).compact.sort
#       if !constituent_exchange_labels.empty?
#         Exchange.where(label: constituent_exchange_labels).update(is_composite_exchange: false, composite_exchange_id: composite_exchange_id)
#       end
#       if composite_exchange
#         composite_exchange.update(is_composite_exchange: true, composite_exchange_id: nil)
#       end
#     end
#   end
# end
