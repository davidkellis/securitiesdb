Sequel.migration do
  change do
    alter_table :security_classifications do
      set_column_default :date, nil
      
      add_index [:classification_id, :security_id, :date], unique: true
    end
  end
end
