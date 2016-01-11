class QuandlBlsImporter < QuandlTimeSeriesImporter
  def import
    # import BLS datasets from https://www.quandl.com/data/BLSE

    # BLSE/CEU0000000001 -> Employment - All employees, thousands; Total nonfarm industry
    # import_quandl_time_series("BLSE", "CEU0000000001")

    import_quandl_time_series_database("BLSE")    # BLS Employment & Unemployment - https://www.quandl.com/data/BLSE
    import_quandl_time_series_database("BLSI")    # BLS Inflation & Prices - https://www.quandl.com/data/BLSI
    import_quandl_time_series_database("BLSB")    # BLS Pay & Benefits - https://www.quandl.com/data/BLSB
    import_quandl_time_series_database("BLSP")    # BLS Productivity - https://www.quandl.com/data/BLSP
  end
end
