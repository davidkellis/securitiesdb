require 'csv'
require 'zip'

class QuandlEodImporter

  def import
    eod_bars = Eod::Client.new.eod_bars
    import_eod_bars(eod_bars)
  end

  private

  def import_eod_bars(eod_bars)
  end

end
