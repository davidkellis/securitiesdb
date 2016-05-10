Sequel.migration do
  change do

    create_table :exchanges do
      primary_key :id
      String :label, size: 50, null: false
      String :name, size: 255, null: false
      String :timezone_name, null: true         # this should be one of the string identifiers listed here: http://www.joda.org/joda-time/timezones.html
      String :currency, size: 64, null: true    # this is an ISO 4217 currency code (see https://en.wikipedia.org/wiki/ISO_4217; e.g. USD, EUR, CHF, etc.)
      Integer :market_open, null: true          # time is an integer of the form hhmmss; if nil, each security traded on exchange has its own open time
      Integer :market_close, null: true         # time is an integer of the form hhmmss; if nil, each security traded on exchange has its own close time
      Integer :trading_window_in_days, null: true  # if trading opens and closes on same day, then this is 1; otherwise, the trading window is the number of calendar days spanned by the open trading window (e.g. 2 if market closes on day following the market open); nil if each security has its own trading trading window

      index :id, unique: true
      index :label, unique: true
    end

    create_table :industries do
      primary_key :id
      String :name, size: 255, null: false

      index :id, unique: true
      index :name, unique: true
    end

    create_table :sectors do
      primary_key :id
      String :name, size: 255, null: false

      index :id, unique: true
      index :name, unique: true
    end

    create_table :security_types do
      primary_key :id
      String :name, size: 255, null: false
      String :classification, size: 255, null: true   # this is a broader classification than the security type itself; e.g. Commodity, Equity, Index, Currency, etc.

      index :id, unique: true
      index :name, unique: true
    end

    create_table :securities do
      primary_key :id
      foreign_key :security_type_id, :security_types, null: true
      foreign_key :industry_id, :industries, null: true
      foreign_key :sector_id, :sectors, null: true
      String :name, size: 255, null: false
      String :search_key, size: 255, null: false

      index :id, unique: true
      index [:security_type_id, :name], unique: true
    end

    create_table :listed_securities do
      primary_key :id
      foreign_key :exchange_id, :exchanges, null: false
      foreign_key :security_id, :securities, null: false
      String :symbol, size: 15, null: false
      Integer :listing_start_date, null: true
      Integer :listing_end_date, null: true

      # String :figi, size: 12, null: true              # figi = financial instrument global identifier - formerly bbgid - bloomberg global id - unique per security per exchange
      # String :composite_figi, size: 12, null: true    # global composite id - unique per security (but shared across exchanges within the same composite exchange)
      Integer :csi_number, null: true                 # CSI Number (identifier from csidata.com)

      Integer :market_open, null: true              # time is an integer of the form hhmmss; if nil, this security is traded during the exchange's common open trading window
      Integer :market_close, null: true             # time is an integer of the form hhmmss; if nil, this security is traded during the exchange's common open trading window
      Integer :trading_window_in_days, null: true   # if trading opens and closes on same day, then this is 1; otherwise, the trading window is the number of calendar days spanned by the open trading window (e.g. 2 if market closes on day following the market open); nil if this security is traded during the exchange's common open trading window

      index :id, unique: true
      # index :figi, unique: true
      index [:exchange_id, :symbol, :listing_start_date]
    end

    # An option is a 5-tuple (underlying security, expiration, strike, callOrPut, americanOrEuropean)
    create_table :options do
      primary_key :id
      foreign_key :security_id, :securities, null: false
      foreign_key :underlying_security_id, :securities, key: :id, null: false

      Integer :expiration, null: false                    # date of the form yyyymmdd
      String :type, fixed: true, size: 1, null: false     # call or put => C or P
      BigDecimal :strike, size: [19, 4], null: false
      String :style, fixed: true, size: 1, null: false    # American or European => A or E

      index :id, unique: true
      index [:underlying_security_id, :expiration, :strike, :type, :style], unique: true
    end

    create_table :eod_option_quotes do
      primary_key :id
      foreign_key :option_id, :options, null: false

      Integer :date, null: false
      BigDecimal :last, size: [19, 4], null: false
      BigDecimal :bid, size: [19, 4], null: false
      BigDecimal :ask, size: [19, 4], null: false
      Integer :volume, null: false
      Integer :open_interest, null: false

      index :id, unique: true
      index [:option_id, :date], unique: true
    end

    create_table :data_vendors do
      primary_key :id
      String :name, size: 255, null: false
    end

    # a table to store update frequencies - daily, weekly, monthly, quarterly, yearly, irregular
    create_table :update_frequencies do
      primary_key :id
      String :label, size: 255, null: false
    end

    create_table :time_series do
      primary_key :id
      foreign_key :data_vendor_id, :data_vendors, null: false
      foreign_key :update_frequency_id, :update_frequencies, null: false    # update frequency indicates which table the observations are stored in
      String :database, size: 255, null: false
      String :dataset, size: 255, null: false
      String :name, size: 255, null: true
      String :description, text: true, null: true

      index :id, unique: true
      index [:data_vendor_id, :database, :dataset], unique: true
    end

    create_table :eod_bars do
      primary_key :id
      foreign_key :security_id, :securities, null: false
      Integer :date, null: false
      BigDecimal :open, :size => [19, 4], null: false
      BigDecimal :high, :size => [19, 4], null: false
      BigDecimal :low, :size => [19, 4], null: false
      BigDecimal :close, :size => [19, 4], null: false
      Bignum :volume, null: false

      index :id, unique: true
      index [:security_id, :date], unique: true
    end

    create_table :corporate_action_types do
      primary_key :id
      String :name, size: 20, null: false

      index :id, unique: true
      index :name, unique: true
    end

    create_table :corporate_actions do
      primary_key :id
      foreign_key :security_id, :securities, null: false
      foreign_key :corporate_action_type_id, :corporate_action_types, null: false
      Integer :ex_date, null: false           # date of the form yyyymmdd - this is the first date in which the corporate action has taken effect
      Integer :declaration_date, null: true   # date of the form yyyymmdd
      Integer :record_date, null: true        # date of the form yyyymmdd
      Integer :payable_date, null: true       # date of the form yyyymmdd

      # NOTE:
      # 1. In accordance with the calculations at http://www.crsp.com/files/data_descriptions_guide_0.pdf,
      # http://www.crsp.com/products/documentation/crsp-calculations,
      # http://www.crsp.com/products/documentation/data-definitions-f#factor-to-adjust-price-in-period,
      # http://www.crsp.com/products/documentation/daily
      # http://www.crsp.com/products/documentation/daily-and-monthly-time-series, and
      # https://www.quandl.com/data/EOD/documentation/methodology,
      # split and dividend adjustment factors are decimal values such that
      # unadjusted price and dividend payout values divided by the appropriate cumulative adjustment factor yield an adjusted price or dividend payout value,
      # and
      # unadjusted share and volume values multiplied by the appropriate cumulative adjustment factor yield an adjusted share or volume value.
      # 2. Cumlative adjustment factors may be computed by multiplying consecutive adjustment factors.
      #
      # NOTE:
      # The adjustment factor for splits and stock dividends is recorded as a decimal approximation of the ratio of (New Float) / (Old Float)
      # The adjustment factor for cash dividends is recorded as (Close Price + Dividend Amount) / (Close Price)
      BigDecimal :adjustment_factor, :size=>[30, 15], null: false

      index :id, unique: true
      index [:corporate_action_type_id, :security_id, :ex_date], unique: true
      index :security_id
    end

    create_table :fundamental_attributes do
      primary_key :id
      String :label, size: 255, null: false
      String :name, size: 255, null: false
      String :description, text: true, null: true

      index :id, unique: true
      index :label, unique: true
    end

    create_table :fundamental_dimensions do
      primary_key :id
      String :name, size: 255, null: false
      String :description, text: true, null: true

      index :id, unique: true
      index :name, unique: true
    end

    create_table :fundamental_datasets do
      primary_key :id
      foreign_key :security_id, :securities, null: false
      foreign_key :fundamental_attribute_id, :fundamental_attributes, null: false
      foreign_key :fundamental_dimension_id, :fundamental_dimensions, null: false
      foreign_key :time_series_id, :time_series, null: false

      index :id, unique: true
      index [:security_id, :fundamental_attribute_id, :fundamental_dimension_id, :time_series_id], unique: true
    end


    # we partition the observations into multiple tables - one table per update frequency (i.e. daily, weekly, monthly, quarterly, yearly, irregular)
    # got the idea from http://andyfiedler.com/blog/tag/time-series/

    create_table :daily_observations do
      primary_key :id
      foreign_key :time_series_id, :time_series, null: false
      Integer :date, null: false    # the date is the day that the observation was made; it is assumed that the observation covers a subset of the time between midnight and 11:59:59 p.m. on one day
      BigDecimal :value, size: [30, 9], null: false

      index :id, unique: true
      index [:time_series_id, :date], unique: true
    end

    create_table :weekly_observations do
      primary_key :id
      foreign_key :time_series_id, :time_series, null: false
      Integer :date, null: false    # the date is the last day of the week-long period over which the observation was made
      BigDecimal :value, size: [30, 9], null: false

      index :id, unique: true
      index [:time_series_id, :date], unique: true
    end

    create_table :monthly_observations do
      primary_key :id
      foreign_key :time_series_id, :time_series, null: false
      Integer :date, null: false    # the date is the last day of the month-long period over which the observation was made
      BigDecimal :value, size: [30, 9], null: false

      index :id, unique: true
      index [:time_series_id, :date], unique: true
    end

    create_table :quarterly_observations do
      primary_key :id
      foreign_key :time_series_id, :time_series, null: false
      Integer :date, null: false    # the date is the last day of the quarter-long period over which the observation was made
      BigDecimal :value, size: [30, 9], null: false

      index :id, unique: true
      index [:time_series_id, :date], unique: true
    end

    create_table :yearly_observations do
      primary_key :id
      foreign_key :time_series_id, :time_series, null: false
      Integer :date, null: false    # the date is the last day of the year-long period over which the observation was made
      BigDecimal :value, size: [30, 9], null: false

      index :id, unique: true
      index [:time_series_id, :date], unique: true
    end

    create_table :irregular_observations do
      primary_key :id
      foreign_key :time_series_id, :time_series, null: false
      Integer :date, null: false
      BigDecimal :value, size: [30, 9], null: false

      index :id, unique: true
      index [:time_series_id, :date], unique: true
    end

  end
end
