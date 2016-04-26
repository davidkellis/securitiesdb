Sequel.migration do
  change do
    alter_table :corporate_actions do
      add_column :value, BigDecimal, null: true   # for dividends, this will hold the dividend amount
    end
  end
end
