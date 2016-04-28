Sequel.migration do
  change do
    alter_table :securities do
      drop_foreign_key :industry_id
      drop_foreign_key :sector_id
    end

    drop_table :industries
    drop_table :sectors

    create_table :classifications do
      primary_key :id
      String :name, text: true, null: false

      index :id, unique: true
      index :name, unique: true
    end

    create_table :security_classifications do
      primary_key :id
      foreign_key :classification_id, :classifications, null: false
      foreign_key :security_id, :securities, null: false
    end
  end
end
