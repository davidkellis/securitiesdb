
Sequel.migration do
  change do

    create_table :exchanges do
      primary_key :id
      String :label, size: 50, null: false
      String :name, size: 255

      TrueClass :is_composite_exchange, null: false
      foreign_key :composite_exchange_id, :exchanges, null: true

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
      String :market_sector, size: 255

      index :id, unique: true
      index :name, unique: true
    end

    create_table :securities do
      primary_key :id
      foreign_key :exchange_id, :exchanges, null: true
      foreign_key :security_type_id, :security_types, null: true
      foreign_key :industry_id, :industries, null: true
      foreign_key :sector_id, :sectors, null: true
      String :name, size: 255
      String :symbol, null: false, size: 15
      String :figi, size: 12              # figi = financial instrument global identifier - formerly bbgid - bloomberg global id - unique per security per exchange
      String :bbgid_composite, size: 12   # bloomberg global composite id - unique per security (but shared across exchanges within the same composite exchange)
      Integer :csi_number, null: true     # CSI Number (identifier from csidata.com)

      # TrueClass :primary_listing, null: false
      Integer :start_date, null: true
      Integer :end_date, null: true

      index :id, unique: true
      index :figi, unique: true
      index [:exchange_id, :symbol, :start_date]
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
      BigDecimal :open, :size=>[12, 2], null: false   # single-digit billions
      BigDecimal :high, :size=>[12, 2], null: false   # single-digit billions
      BigDecimal :low, :size=>[12, 2], null: false    # single-digit billions
      BigDecimal :close, :size=>[12, 2], null: false  # single-digit billions
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
      Integer :ex_date, null: false
      Integer :declaration_date
      Integer :record_date
      Integer :payable_date

      # NOTE:
      # 1. In accordance with the calculations at http://www.crsp.com/files/data_descriptions_guide_0.pdf,
      # http://www.crsp.com/products/documentation/crsp-calculations,
      # http://www.crsp.com/products/documentation/data-definitions-f#factor-to-adjust-price-in-period,
      # http://www.crsp.com/products/documentation/daily
      # http://www.crsp.com/products/documentation/daily-and-monthly-time-series, and
      # https://www.quandl.com/data/EOD/documentation/methodology,
      # split and dividend adjustment factors are decimal values such that
      # when unadjusted price and dividend payout values are divided by the appropriate cumulative adjustment factor yield an adjusted price or dividend payout value,
      # and
      # when unadjusted share and volume values are multiplied by the appropriate cumulative adjustment factor yield an adjusted share or volume value.
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
      String :description, :text => true, null: true

      index :id, unique: true
      index :label, unique: true
    end

    create_table :fundamental_dimensions do
      primary_key :id
      String :name, size: 255, null: false
      String :description, :text => true, null: true

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
      Integer :date, null: false
      BigDecimal :value, size: [30, 9], null: false

      index :id, unique: true
      index [:time_series_id, :date], unique: true
    end

    create_table :weekly_observations do
      primary_key :id
      foreign_key :time_series_id, :time_series, null: false
      Integer :date, null: false
      BigDecimal :value, size: [30, 9], null: false

      index :id, unique: true
      index [:time_series_id, :date], unique: true
    end

    create_table :monthly_observations do
      primary_key :id
      foreign_key :time_series_id, :time_series, null: false
      Integer :date, null: false
      BigDecimal :value, size: [30, 9], null: false

      index :id, unique: true
      index [:time_series_id, :date], unique: true
    end

    create_table :quarterly_observations do
      primary_key :id
      foreign_key :time_series_id, :time_series, null: false
      Integer :date, null: false
      BigDecimal :value, size: [30, 9], null: false

      index :id, unique: true
      index [:time_series_id, :date], unique: true
    end

    create_table :yearly_observations do
      primary_key :id
      foreign_key :time_series_id, :time_series, null: false
      Integer :date, null: false
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
