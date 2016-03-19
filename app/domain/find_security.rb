require 'date'

class FindSecurity
  def self.us_stocks
    @us_stocks ||= FindSecurity.new(Exchange.us_stock_exchanges.to_a)
  end

  def self.us_indices
    @us_indices ||= FindSecurity.new(Exchange.cboe.to_a)
  end


  def initialize(exchange_or_exchanges = [])
    @exchanges = [exchange_or_exchanges].flatten.uniq
  end

  def run(symbol, datestamp = current_datestamp())
    # 1. if not found in (1), search in the appropriate constituent (local) exchange(s)
    if @exchanges && !@exchanges.empty?
      listed_securities = if datestamp
        ListedSecurity.
          where(symbol: symbol, exchange_id: @exchanges.map(&:id)).
          where { (listing_start_date =~ nil) | ((listing_start_date <= datestamp) & (listing_end_date >= datestamp)) }.
          to_a
      else
        ListedSecurity.
          where(symbol: symbol, exchange_id: @exchanges.map(&:id)).
          to_a
      end

      case listed_securities.count
      when 0
        nil
      when 1
        listed_securities.security.first
      else
        securities = listed_securities.map(&:security).uniq
        case securities.count
        when 0
          listed_securities.first.security
        when 1
          securities.first
        else
          # todo: figure out which exchange is preferred, and then return the security in the most preferred exchange
          raise "Symbol #{symbol} is listed in multiple exchanges: #{securities_in_local_exchanges.map(&:exchange).map(&:label)}"
        end
      end
    end
  end

  private

  def current_datestamp
    Date.today.strftime("%Y%m%d")
  end
end
