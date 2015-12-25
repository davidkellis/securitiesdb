class LookupSecurity
  def initialize(composite_exchange, local_exchanges, catch_all_exchange)
    @composite_exchange = composite_exchange
    @local_exchanges = local_exchanges
    @catch_all_exchange = catch_all_exchange
  end

  # search for the security
  # (1) in the appropriate composite exchange,
  # (2) in the appropriate constituent (local) exchanges,
  # and finally (3) in the catch-all exchange
  def run(symbol, date)
    # 1. search for the security in the appropriate composite exchange
    begin
      Security.first(symbol: symbol, exchange_id: @composite_exchange.id)
    end ||
    # 2. if not found in (1), search in the appropriate constituent (local) exchange(s)
    begin
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
    end ||
    # 3. if not found in (1) or (2), search in the appropriate catch-all exchange(s)
    begin
      securities_in_catch_all_exchanges = Security.
                                            where(symbol: symbol, exchange_id: @catch_all_exchange.id).
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
end
