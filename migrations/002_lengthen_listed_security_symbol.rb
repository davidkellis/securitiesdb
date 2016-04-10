Sequel.migration do
  change do
    alter_table :listed_securities do
      set_column_type :symbol, String, size: 21   # we need to support OCC option symbols, which are 21 chars long
    end
  end
end
