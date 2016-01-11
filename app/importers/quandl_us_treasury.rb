class QuandlUsTreasuryImporter < QuandlTimeSeriesImporter
  def import
    # The U.S. Treasury ensures the nation's financial security, manages the nation's debt, collects tax revenues, and issues currency, provides data on yield rates.
    import_quandl_time_series_database("USTREASURY")    # Treasury rates, yield curve rates, debt, tax revenues, etc. - https://www.quandl.com/data/USTREASURY
  end
end
