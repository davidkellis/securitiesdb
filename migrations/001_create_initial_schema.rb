
Sequel.migration do
  change do

    create_table :exchanges do
      primary_key :id
      String :label, :size => 50, :null => false
      String :name, :size => 255

      index :id, :unique => true
      index :label, :unique => true
    end

    create_table :industries do
      primary_key :id
      String :name, :size => 255, :null => false

      index :id, :unique => true
      index :name, :unique => true
    end

    create_table :sectors do
      primary_key :id
      String :name, :size => 255, :null => false

      index :id, :unique => true
      index :name, :unique => true
    end

    create_table :security_types do
      primary_key :id
      String :name, :size => 255, :null => false
      String :market_sector, :size => 255

      index :id, :unique => true
      index :name, :unique => true
    end

    create_table :securities do
      primary_key :id
      foreign_key :exchange_id, :exchanges, :null => true
      foreign_key :security_type_id, :security_types, :null => true
      foreign_key :industry_id, :industries, :null => true
      foreign_key :sector_id, :sectors, :null => true
      String :name, :size => 255
      String :symbol, :null => false, :size => 15
      String :figi, :size => 12       # figi = financial instrument global identifier - formerly bbgid - bloomberg global id - unique per security per exchange
      String :bbgcid, :size => 12     # bloomberg global composite id - unique per security (but shared across exchanges)

      Integer :start_date
      Integer :end_date, :null => true

      index :id, :unique => true
      index :figi, :unique => true
      index [:exchange_id, :symbol, :start_date]
    end

    create_table :eod_bars do
      primary_key :id
      foreign_key :security_id, :securities, :null => false
      Bignum :start_time, :null => false
      Bignum :end_time, :null => false
      BigDecimal :open, :size=>[12, 2], :null => false   # single-digit billions
      BigDecimal :high, :size=>[12, 2], :null => false   # single-digit billions
      BigDecimal :low, :size=>[12, 2], :null => false    # single-digit billions
      BigDecimal :close, :size=>[12, 2], :null => false  # single-digit billions
      Bignum :volume, :null => false

      index :id, :unique => true
      index :security_id
      index [:security_id, :start_time], :unique => true
    end

    create_table :corporate_action_types do
      primary_key :id
      String :name, :size => 20, :null => false

      index :id, :unique => true
    end

    # todo - finish this table
    create_table :corporate_actions do
      primary_key :id
      foreign_key :security_id, :securities, :null => false
      foreign_key :corporate_action_type_id, :corporate_action_types, :null => false
      Integer :ex_date, :null => false
      Integer :declaration_date
      Integer :record_date
      Integer :payable_date
      BigDecimal :adjustment_ratio, :size=>[30, 15], :null => false     # splits are recorded as a decimal approximation of the ratio of "new float" / "old float"

      index :id, :unique => true
      index [:corporate_action_type_id, :security_id, :ex_date], :unique => true
      index :security_id
    end

    create_table :quarterly_reports do
      primary_key :id
      foreign_key :security_id, :securities, :null => false
      Bignum :start_time, :null => false
      Bignum :end_time, :null => false
      Bignum :publication_time, :null => false
      File :income_statement, :null => false
      File :balance_sheet, :null => false
      File :cash_flow_statement, :null => false

      index :id, :unique => true
      index :security_id
      index :publication_time
      index [:security_id, :end_time], :unique => true
    end

    create_table :annual_reports do
      primary_key :id
      foreign_key :security_id, :securities, :null => false
      Bignum :start_time, :null => false
      Bignum :end_time, :null => false
      Bignum :publication_time, :null => false
      File :income_statement, :null => false
      File :balance_sheet, :null => false
      File :cash_flow_statement, :null => false

      index :id, :unique => true
      index :security_id
      index :publication_time
      index [:security_id, :end_time], :unique => true
    end

    create_table :strategies do
      primary_key :id
      String :name, :size => 255, :null => false

      index :id, :unique => true
      index :name, :unique => true
    end

    create_table :trial_sets do
      primary_key :id
      foreign_key :strategy_id, :strategies, :null => false
      BigDecimal :principal, :size=>[30, 2]
      BigDecimal :commission_per_trade, :size=>[30, 2]
      BigDecimal :commission_per_share, :size=>[30, 2]
      String :duration, :size => 12

      index :id, :unique => true
      index :strategy_id
    end

    create_join_table(:trial_set_id => :trial_sets, :security_id => :securities)    # creates securities_trial_sets join table

    create_table :trials do
      primary_key :id
      foreign_key :trial_set_id, :trial_sets, :null => false
      Bignum :start_time, :null => false
      Bignum :end_time, :null => false
      File :transaction_log, :null => false
      File :portfolio_value_log, :null => false

      BigDecimal :yield
      BigDecimal :mfe           # maximum favorable excursion
      BigDecimal :mae           # maximum adverse excursion
      BigDecimal :daily_std_dev

      index :id, :unique => true
      index :trial_set_id
    end

    create_table :trial_set_distribution_types do
      primary_key :id
      String :name, :null => false
    end

    create_table :trial_set_distributions do
      primary_key :id
      foreign_key :trial_set_id, :trial_sets, :null => false
      foreign_key :trial_set_distribution_type_id, :trial_set_distribution_types, :null => false
      String :attribute, :null => false
      Bignum :start_time, :null => false
      Bignum :end_time, :null => false
      File :distribution, :null => false

      Integer :n
      BigDecimal :average
      BigDecimal :min
      BigDecimal :max
      BigDecimal :percentile_1
      BigDecimal :percentile_5
      BigDecimal :percentile_10
      BigDecimal :percentile_15
      BigDecimal :percentile_20
      BigDecimal :percentile_25
      BigDecimal :percentile_30
      BigDecimal :percentile_35
      BigDecimal :percentile_40
      BigDecimal :percentile_45
      BigDecimal :percentile_50
      BigDecimal :percentile_55
      BigDecimal :percentile_60
      BigDecimal :percentile_65
      BigDecimal :percentile_70
      BigDecimal :percentile_75
      BigDecimal :percentile_80
      BigDecimal :percentile_85
      BigDecimal :percentile_90
      BigDecimal :percentile_95
      BigDecimal :percentile_99
    end

    create_table :sampling_distributions do
      primary_key :id
      foreign_key :trial_set_distribution_id, :trial_set_distributions, :null => false
      String :sample_statistic, :null => false
      File :distribution, :null => false

      Integer :n
      BigDecimal :average
      BigDecimal :min
      BigDecimal :max
      BigDecimal :percentile_1
      BigDecimal :percentile_5
      BigDecimal :percentile_10
      BigDecimal :percentile_15
      BigDecimal :percentile_20
      BigDecimal :percentile_25
      BigDecimal :percentile_30
      BigDecimal :percentile_35
      BigDecimal :percentile_40
      BigDecimal :percentile_45
      BigDecimal :percentile_50
      BigDecimal :percentile_55
      BigDecimal :percentile_60
      BigDecimal :percentile_65
      BigDecimal :percentile_70
      BigDecimal :percentile_75
      BigDecimal :percentile_80
      BigDecimal :percentile_85
      BigDecimal :percentile_90
      BigDecimal :percentile_95
      BigDecimal :percentile_99
    end


    alter_table :trial_set_distributions do
      add_index :id, :unique => true
      add_index :trial_set_id
      add_index :trial_set_distribution_type_id
    end

    create_table :sample_statistics do
      primary_key :id
      String :name, :null => false

      index :id, :unique => true
    end

    alter_table :sampling_distributions do
      drop_column :sample_statistic
      add_foreign_key :sample_statistic_id, :sample_statistics, :null => false

      add_index :id, :unique => true
      add_index :trial_set_distribution_id
      add_index :sample_statistic_id
    end

  end
end
