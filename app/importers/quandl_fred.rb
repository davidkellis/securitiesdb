class QuandlFredImporter < QuandlTimeSeriesImporter
  def import
    # import FRED datasets from https://www.quandl.com/data/FRED

    import_quandl_time_series_database("FRED")    # Federal Reserve Economic Data - https://www.quandl.com/data/FRED
  end
end
