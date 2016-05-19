Sequel.migration do
  change do
    create_table :security_variables do
      primary_key :id
      String :name, text: true, null: false
    end

    create_table :classification_summaries do
      primary_key :id
      foreign_key :classification_id, :classifications, null: false
      foreign_key :security_variable_id, :security_variables, null: false
      Integer :date, null: false

      Integer :n
      BigDecimal :mean
      BigDecimal :variance
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
  end
end
