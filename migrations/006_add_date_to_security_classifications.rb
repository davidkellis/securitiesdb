Sequel.migration do
  change do
    alter_table :security_classifications do
      add_column :date, Integer, null: false, default: 19000101
    end
  end
end
