class LookupSecurity
  def self.us_stocks
    @us_stocks ||= LookupSecurity.new(Exchange.us_composite, Exchange.us_stock_exchanges.to_a, Exchange.catch_all_stock)
  end

  def self.us_indices
    @us_indices ||= LookupSecurity.new(nil, Exchange.cboe.to_a, Exchange.indices)
  end


  # any of the 3 arguments may be nil
  # if supplied, local_exchange_or_exchanges and catch_all_exchange_or_exchanges are both arrays of Exchanges
  def initialize(composite_exchange, local_exchange_or_exchanges, catch_all_exchange_or_exchanges)
    @composite_exchange = composite_exchange
    @local_exchanges = [local_exchange_or_exchanges].flatten
    @catch_all_exchanges = [catch_all_exchange_or_exchanges].flatten
  end

  # search for the security
  # (1) in the appropriate composite exchange,
  # (2) in the appropriate constituent (local) exchanges,
  # and finally (3) in the catch-all exchanges
  def run(symbol, date = nil)
    # 1. search for the security in the appropriate composite exchange
    begin
      if @composite_exchange
        Security.first(symbol: symbol, exchange_id: @composite_exchange.id)
      end
    end ||
    # 2. if not found in (1), search in the appropriate constituent (local) exchange(s)
    begin
      if @local_exchanges && !@local_exchanges.empty?
        securities_in_local_exchanges = Security.where(symbol: symbol, exchange_id: @local_exchanges.map(&:id)).to_a
        case securities_in_local_exchanges.count
        when 0
          nil
        when 1
          securities_in_local_exchanges.first
        else
          # todo: figure out which exchange is preferred, and then return the security in the most preferred exchange
          raise "Symbol #{symbol} is listed in multiple exchanges: #{securities_in_local_exchanges.map(&:exchange).map(&:label)}"
        end
      end
    end ||
    # 3. if not found in (1) or (2), search in the appropriate catch-all exchange(s)
    begin
      if @catch_all_exchanges && !@catch_all_exchanges.empty?
        securities_in_catch_all_exchanges = if date
          Security.
            where(symbol: symbol, exchange_id: @catch_all_exchanges.map(&:id)).
            where { (start_date <= date) & (end_date >= date) }.
            to_a
        else
          Security.
            where(symbol: symbol, exchange_id: @catch_all_exchanges.map(&:id)).
            to_a
        end

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
  end
end
