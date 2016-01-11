class QuandlUsCensusImporter < QuandlTimeSeriesImporter
  def import
    # Data on the American people, places and economy. It provides many data on U.S. imports/exports, domestic production, and other key national indicators.
    import_quandl_time_series_database("USCENSUS")    # Census stats - https://www.quandl.com/data/USCENSUS
  end
end
