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
      String :major, text: true, null: false    # primary classification
      String :minor, text: true, null: false    # sub-classification
      String :micro, text: true, null: false    # sub-sub-classification

      index :id, unique: true
      index [:major, :minor, :micro], unique: true
      index :minor
      index :micro
    end

    create_table :security_classifications do
      primary_key :id
      foreign_key :classification_id, :classifications, null: false
      foreign_key :security_id, :securities, null: false
    end
  end
end
