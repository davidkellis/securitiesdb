Sequel.migration do
  change do
    alter_table :time_series do
      set_column_type :name, String, text: true
    end
  end
end
