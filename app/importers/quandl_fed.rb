class QuandlFedImporter < QuandlTimeSeriesImporter
  def import
    # Official US figures on money supply, interest rates, mortgages, government finances, bank assets and debt, exchange rates, industrial production.
    import_quandl_time_series_database("FED")
  end
end
