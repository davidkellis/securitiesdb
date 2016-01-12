
Sequel.migration do
  change do

    # An option is a 5-tuple (underlying security, expiration, strike, callOrPut, americanOrEuropean)
    create_table :options do
      primary_key :id
      foreign_key :security_id, :securities, null: false
      foreign_key :underlying_security_id, :securities, key: :id, null: false

      Integer :expiration, null: false                    # date of the form yyyymmdd
      String :type, fixed: true, size: 1, null: false     # call or put => C or P
      BigDecimal :strike, size: [30, 9], null: false
      String :style, fixed: true, size: 1, null: false    # American or European => A or E

      index :id, unique: true
      index [:underlying_security_id, :expiration, :strike, :type, :style], unique: true
    end

    create_table :eod_option_quotes do
      primary_key :id
      foreign_key :option_id, :options, null: false

      Integer :date, null: false
      BigDecimal :last, size: [30, 9], null: false
      BigDecimal :bid, size: [30, 9], null: false
      BigDecimal :ask, size: [30, 9], null: false
      Integer :volume, null: false
      Integer :open_interest, null: false

      index :id, unique: true
      index [:option_id, :date], unique: true
    end

  end
end
