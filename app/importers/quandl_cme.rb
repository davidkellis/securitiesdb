class QuandlCmeImporter < QuandlTimeSeriesImporter
  def import
    # import CME datasets from https://www.quandl.com/data/CME

    import_quandl_time_series_database("CME")    # Chicago Mercantile Exchange Futures Data - https://www.quandl.com/data/CME
  end
end
