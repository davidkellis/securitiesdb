require 'date'
require 'pp'

class SecurityImporter
  def initialize
    @exchange_memo = {}
  end

  def import
    Database.connect

    security_downloader = BloombergSecurityDownloader.new

    import_exchanges(security_downloader.exchanges)

    exchanges_to_import = Exchange.us_exchanges
    exchange_labels = exchanges_to_import.map(&:label)
    selection_predicate = ->(security_record) { exchange_labels.include? security_record.exchange_label }

    import_securities(security_downloader.stocks.select(&selection_predicate), Stock)
    import_securities(security_downloader.etps.select(&selection_predicate), Etp)
    import_securities(security_downloader.funds.select(&selection_predicate), Fund)
    import_securities(security_downloader.indices.select(&selection_predicate), Index)

    import_custom_securities
  end

  def import_exchanges(exchanges)
    puts "Importing exchanges."
    exchanges.each do |exchange|
      existing_exchange = Exchange.first(label: exchange.label)
      begin
        if existing_exchange
          existing_exchange.update(exchange.values)
        else
          exchange.save
        end
      rescue Sequel::ValidationFailed, Sequel::HookFailed => e
        puts "Can't import #{exchange.inspect}: #{e.message}"
        puts "Exchange errors: #{exchange.errors.full_messages.join("\n")}"
      end
    end

    puts "Creating user-defined exchanges"
    create_or_update_exchange("DKE", "DKE")
  end

  def create_or_update_exchange(name, label)
    existing_exchange = Exchange.first(label: label)
    begin
      if existing_exchange
        existing_exchange.update(name: name, label: label)
      else
        Exchange.create(name: name, label: label)
      end
    rescue => e
      puts "Can't import exchange (name=#{name} label=#{label}): #{e.message}"
    end
  end

  def import_securities(security_records, security_class)
    puts "Importing #{security_class.name} securities."
    security_records.each {|security_record| import_security(security_record, security_class) }
  end

  def import_security(security_record, security_class)
    existing_security = security_class.where(:figi => security_record.figi).first
    if existing_security
      # puts "Updating: #{security.exchange.name} - #{security.symbol} (#{security.figi})"
      update_security(existing_security, security_record)
    else
      # puts "Creating: #{security.exchange.name} - #{security.symbol} (#{security.figi})"
      create_security(security_record, security_class)
    end
  rescue Sequel::ValidationFailed, Sequel::HookFailed => e
    puts "Can't import #{security_record.inspect}: #{e.message}"
  rescue => e
    puts "Can't import #{security_record.inspect}: #{e.message}"
    puts e.backtrace.join("\n")
  end

  def update_security(security, security_record)
    exchange = lookup_exchange(security_record.exchange_label)
    security.update(
      figi: security_record.figi,
      bb_gcid: security_record.bb_gcid,
      name: security_record.name,
      symbol: security_record.symbol
    )
    security.exchange = exchange if exchange
  end

  def create_security(security_record, security_class)
    exchange = lookup_exchange(security_record.exchange_label)
    security_class.create(
      figi: security_record.figi,
      bb_gcid: security_record.bb_gcid,
      name: security_record.name,
      symbol: security_record.symbol,
      exchange: exchange ? exchange : []
    )
  end

  def lookup_exchange(label)
    @exchange_memo[label] ||= Exchange.where(label: label).first
  end

  def import_custom_securities
    puts "Importing user-defined securities."
    create_or_update_security(Index, "CBOE", "BBGDKE1", "BBGDKE1", "CBOE 1 Month SPX Volatility Index", "^VIX")
    create_or_update_security(Index, "CBOE", "BBGDKE2", "BBGDKE2", "CBOE 3 Month SPX Volatility Index", "^VXV")
  end

  def create_or_update_security(security_class, exchange_label, figi, bb_gcid, name, symbol)
    exchange = lookup_exchange(exchange_label)
    existing_security = security_class.where(:figi => figi).first
    if existing_security
      existing_security.update(
        figi: figi,
        bb_gcid: bb_gcid,
        name: name,
        symbol: symbol
      )
      existing_security.exchange = exchange if exchange
    else
      security_class.create(
        figi: figi,
        bb_gcid: bb_gcid,
        name: name,
        symbol: symbol,
        exchange: exchange ? exchange : []
      )
    end
  end
end

def main
  SecurityImporter.new.import
end

main if __FILE__ == $0
