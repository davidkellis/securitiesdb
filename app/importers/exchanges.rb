require 'date'
require 'pp'

class ExchangesImporter
  def initialize
    @exchange_memo = {}
    @security_type_memo = {}
  end

  def log(msg)
    Application.logger.info("#{Time.now} - #{msg}")
  end

  def import
    import_exchanges
  end

  def import_exchanges
    log "Creating exchanges."

    # NYSE
    create_or_update_exchange("New York Stock Exchange", "NYSE", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)
    create_or_update_exchange("NYSE MKT", "NYSE-MKT", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)
    create_or_update_exchange("NYSE ARCA", "NYSE-ARCA", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)
    create_or_update_exchange("NYSE ARCA Options", "NYSE-ARCA-OPT", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)
    create_or_update_exchange("NYSE AMEX Options", "NYSE-AMEX-OPT", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)
    create_or_update_exchange("NYSE Bonds", "NYSE-BONDS", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)
    create_or_update_exchange("NYSE Catch All", "NYSE-CATCHALL", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)

    # NASDAQ
    create_or_update_exchange("NASDAQ Capital Market", "NASDAQ-CM", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)
    create_or_update_exchange("NASDAQ Global Market", "NASDAQ-GM", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)
    create_or_update_exchange("NASDAQ Global Select Market", "NASDAQ-GSM", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)
    create_or_update_exchange("NASDAQ OMX PHLX", "NASDAQ-PHLX", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)
    create_or_update_exchange("NASDAQ OMX BX", "NASDAQ-BX", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)
    create_or_update_exchange("NASDAQ OMX BX", "NASDAQ-BX", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)
    create_or_update_exchange("NASDAQ OMX PSX", "NASDAQ-PSX", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)
    create_or_update_exchange("NASDAQ Catch All", "NASDAQ-CATCHALL", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)

    # BATS
    create_or_update_exchange("BATS BZX", "BATS-BZX", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)
    create_or_update_exchange("BATS BYX", "BATS-BYX", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)
    create_or_update_exchange("BATS EDGA", "BATS-EDGA", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)   # formerly Direct Edge EDGA
    create_or_update_exchange("BATS EDGX", "BATS-EDGX", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)   # formerly Direct Edge EDGX
    create_or_update_exchange("BATS BZX Options", "BATS-EDGX-OPT", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)
    create_or_update_exchange("BATS EDGX Options", "BATS-EDGX-OPT", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)
    create_or_update_exchange("BATS Catch All", "BATS-CATCHALL", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)

    # OTC Markets
    create_or_update_exchange("OTC Markets QX", "OTC-QX", TimeZone::US_EASTERN_TIME, Currency::USD, 60000, 170000, 1)
    create_or_update_exchange("OTC Markets QB", "OTC-QB", TimeZone::US_EASTERN_TIME, Currency::USD, 60000, 170000, 1)
    create_or_update_exchange("OTC Markets Pink", "OTC-PINK", TimeZone::US_EASTERN_TIME, Currency::USD, 60000, 170000, 1)
    create_or_update_exchange("OTC Markets Catch All", "OTC-CATCHALL", TimeZone::US_EASTERN_TIME, Currency::USD, 60000, 170000, 1)

    # FINRA's OTCBB
    create_or_update_exchange("OTC Bulletin Board", "OTCBB", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)

    # ICE
    create_or_update_exchange("ICE Futures U.S.", "ICE-FUT-US", TimeZone::US_EASTERN_TIME, Currency::USD, nil, nil, nil)
    create_or_update_exchange("ICE Futures Europe", "ICE-FUT-EUR", TimeZone::US_EASTERN_TIME, Currency::USD, nil, nil, nil)
    create_or_update_exchange("ICE Futures Canada", "ICE-FUT-CA", TimeZone::US_EASTERN_TIME, Currency::USD, nil, nil, nil)
    create_or_update_exchange("ICE Futures Singapore", "ICE-FUT-SG", TimeZone::US_EASTERN_TIME, Currency::USD, nil, nil, nil)
    create_or_update_exchange("ICE Endex", "ICE-ENDEX", TimeZone::US_EASTERN_TIME, Currency::USD, nil, nil, nil)

    # CBOE
    create_or_update_exchange("Chicago Board Options Exchange", "CBOE", TimeZone::US_CENTRAL_TIME, Currency::USD, nil, nil, nil)

    # CME Group
    create_or_update_exchange("Chicago Mercantile Exchange", "CME", TimeZone::US_CENTRAL_TIME, Currency::USD, 180000, 170000, 2)
    create_or_update_exchange("Chicago Board of Trade", "CBOT", TimeZone::US_CENTRAL_TIME, Currency::USD, 180000, 170000, 2)
    create_or_update_exchange("New York Mercantile Exchange", "NYMEX", TimeZone::US_EASTERN_TIME, Currency::USD, 180000, 170000, 2)
    create_or_update_exchange("Commodity Exchange, Inc", "COMEX", TimeZone::US_EASTERN_TIME, Currency::USD, 180000, 170000, 2)


    log "Creating catch-all exchanges."

    create_or_update_exchange("OTC", "OTC", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)
    create_or_update_exchange("Stocks", "STOCK", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)
    create_or_update_exchange("ETPs", "ETP", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)
    create_or_update_exchange("Indices", "INDEX", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)
    create_or_update_exchange("Mutual Funds", "MUTUAL", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)
    create_or_update_exchange("Options", "OPTION", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)
    create_or_update_exchange("Futures", "FUTURE", TimeZone::US_EASTERN_TIME, Currency::USD, 93000, 160000, 1)
  end

  def create_or_update_exchange(name, label, timezone_name, currency, market_open, market_close, trading_window_in_days)
    existing_exchange = Exchange.first(label: label)
    begin
      if existing_exchange
        existing_exchange.update(
          name: name,
          label: label,
          timezone_name: timezone_name,
          currency: currency,
          market_open: market_open,
          market_close: market_close,
          trading_window_in_days: trading_window_in_days
          # is_composite_exchange: false
        )
      else
        Exchange.create(
          name: name,
          label: label,
          timezone_name: timezone_name,
          currency: currency,
          market_open: market_open,
          market_close: market_close,
          trading_window_in_days: trading_window_in_days
          # is_composite_exchange: false
        )
      end
    rescue => e
      log "Can't import exchange (name=#{name} label=#{label} timezone_name=#{timezone_name} currency=#{currency} market_open=#{market_open} market_close=#{market_close}): #{e.message}"
    end
  end
end
