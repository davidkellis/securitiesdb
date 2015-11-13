
Sequel.migration do
  change do

    create_table :exchanges do
      primary_key :id
      String :label, size: 50, null: false
      String :name, size: 255

      TrueClass :composite_exchange, null: false
      String :constituent_exchange_labels, size: 255        # this is a comma-delimited-list of exchange labels that represent this composite exchange's constituent local exchanges

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
      String :figi, size: 12       # figi = financial instrument global identifier - formerly bbgid - bloomberg global id - unique per security per exchange
      String :bbgcid, size: 12     # bloomberg global composite id - unique per security (but shared across exchanges)

      TrueClass :primary_listing, null: false
      Integer :start_date, null: true
      Integer :end_date, null: true

      index :id, unique: true
      index :figi, unique: true
      index [:exchange_id, :symbol, :start_date]
    end

    create_table :eod_bars do
      primary_key :id
      foreign_key :security_id, :securities, null: false
      Bignum :start_time, null: false
      Bignum :end_time, null: false
      BigDecimal :open, :size=>[12, 2], null: false   # single-digit billions
      BigDecimal :high, :size=>[12, 2], null: false   # single-digit billions
      BigDecimal :low, :size=>[12, 2], null: false    # single-digit billions
      BigDecimal :close, :size=>[12, 2], null: false  # single-digit billions
      Bignum :volume, null: false

      index :id, unique: true
      index :security_id
      index [:security_id, :start_time], unique: true
    end

    create_table :corporate_action_types do
      primary_key :id
      String :name, size: 20, null: false

      index :id, unique: true
    end

    create_table :corporate_actions do
      primary_key :id
      foreign_key :security_id, :securities, null: false
      foreign_key :corporate_action_type_id, :corporate_action_types, null: false
      Integer :ex_date, null: false
      Integer :declaration_date
      Integer :record_date
      Integer :payable_date
      BigDecimal :adjustment_ratio, :size=>[30, 15], null: false     # splits are recorded as a decimal approximation of the ratio of "new float" / "old float"

      index :id, unique: true
      index [:corporate_action_type_id, :security_id, :ex_date], unique: true
      index :security_id
    end

    create_table :fundamental_attributes do
      primary_key :id
      String :name, size: 255, null: false
      String :description, :text => true, null: true

      index :id, unique: true
      index :name, unique: true
    end

    create_table :fundamentals do
      primary_key :id
      foreign_key :security_id, :securities, null: false
      foreign_key :fundamental_attribute_id, :fundamental_attributes, null: false
      BigDecimal :value, :size=>[30, 9], null: false
      Integer :start_date, null: false

      index :id, unique: true
      index [:security_id, :fundamental_attribute_id, :start_date], unique: true
      index [:fundamental_attribute_id, :security_id, :start_date]
    end

    # create_table :quarterly_reports do
    #   primary_key :id
    #   foreign_key :security_id, :securities, null: false
    #   Bignum :start_time, null: false
    #   Bignum :end_time, null: false
    #   Bignum :publication_time, null: false
    #   File :income_statement, null: false
    #   File :balance_sheet, null: false
    #   File :cash_flow_statement, null: false
    #
    #   index :id, unique: true
    #   index :security_id
    #   index :publication_time
    #   index [:security_id, :end_time], unique: true
    # end
    #
    # create_table :annual_reports do
    #   primary_key :id
    #   foreign_key :security_id, :securities, null: false
    #   Bignum :start_time, null: false
    #   Bignum :end_time, null: false
    #   Bignum :publication_time, null: false
    #   File :income_statement, null: false
    #   File :balance_sheet, null: false
    #   File :cash_flow_statement, null: false
    #
    #   index :id, unique: true
    #   index :security_id
    #   index :publication_time
    #   index [:security_id, :end_time], unique: true
    # end

  end
end
