require 'date'

class FindSecurity
  class << self
    def all_exchanges
      @all_exchanges ||= FindSecurity.new(Exchange.all)
    end

    def us_exchanges
      @us_exchanges ||= FindSecurity.new(Exchange.us_exchanges.to_a)
    end

    def us_stocks
      @us_stocks ||= FindSecurity.new(Exchange.us_stock_exchanges.to_a)
    end

    def us_indices
      @us_indices ||= FindSecurity.new(Exchange.indices.to_a)
    end
  end


  def initialize(exchange_or_exchanges = [])
    @exchanges = [exchange_or_exchanges].flatten.uniq
  end

  def all(symbol, datestamp = current_datestamp())
    if @exchanges && !@exchanges.empty?
      listed_securities = find_listed_securities(symbol, datestamp)
      listed_securities.map(&:security).uniq
    end || []
  end

  def one(symbol, datestamp = current_datestamp())
    if @exchanges && !@exchanges.empty?
      listed_securities = find_listed_securities(symbol, datestamp)

      if listed_securities.count <= 1
        listed_securities.map(&:security).first
      else
        securities = listed_securities.map(&:security).uniq
        case securities.count
        when 0
          listed_securities.first.security
        when 1
          securities.first
        else
          identified_securities = listed_securities.map {|listed_security| "listed_security_id=#{listed_security.id} symbol=#{listed_security.symbol} listing_start_date=#{listed_security.listing_start_date} listing_end_date=#{listed_security.listing_end_date} exchange=#{listed_security.exchange.label} security_id=#{listed_security.security.id} name=#{listed_security.security.name}" }
          raise "Symbol #{symbol} identifies multiple securities:\n#{identified_securities.join("\n")}"
        end
      end
    end
  end

  private

  def current_datestamp
    Date.today.strftime("%Y%m%d")
  end

  def find_listed_securities(symbol, datestamp)
    if datestamp
      ListedSecurity.
        where(symbol: symbol, exchange_id: @exchanges.map(&:id)).
        where { (listing_start_date =~ nil) | ((listing_start_date <= datestamp) & (listing_end_date >= datestamp)) }.
        to_a
    else
      ListedSecurity.
        where(symbol: symbol, exchange_id: @exchanges.map(&:id)).
        to_a
    end
  end
end
