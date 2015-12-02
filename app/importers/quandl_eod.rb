require 'csv'
require 'zip'

class QuandlEodImporter

  def import
    eod_bars = QuandlEod::Client.new.all_eod_bars
    import_eod_bars_splits_and_dividends(eod_bars)
  end

  private

  def import_eod_bars_splits_and_dividends(all_eod_bars)
    all_eod_bars.each do |symbol, eod_bars|
      # eod_bars is an array of QuandlEod::EodBar objects; each has the following fields:
      #   date,
      #   unadjusted_open,
      #   unadjusted_high,
      #   unadjusted_low,
      #   unadjusted_close,
      #   unadjusted_volume,
      #   dividend,
      #   split_adjustment_factor,
      #   adjusted_open,
      #   adjusted_high,
      #   adjusted_low,
      #   adjusted_close,
      #   adjusted_volume
      security = Security.first(symbol: symbol, exchange_id: Exchange.us_stock_exchanges.map(&:id))
      most_recent_eod_bar = security.eod_bars_dataset.reverse_order(:date).first

      if most_recent_eod_bar
        import_missing_eod_bars_splits_and_dividends(security, eod_bars.select {|eod_bar| eod_bar.date > most_recent_eod_bar.date })
      else
        import_missing_eod_bars_splits_and_dividends(security, eod_bars)
      end
    end
  end

  # eod_bars is an array of QuandlEod::EodBar objects
  def import_missing_eod_bars_splits_and_dividends(security, eod_bars)
    eod_bars.each do |eod_bar|
      EodBar.create(
        security_id: security.id,
        date: eod_bar.date,
        open: eod_bar.unadjusted_open,
        high: eod_bar.unadjusted_high,
        low: eod_bar.unadjusted_low,
        close: eod_bar.unadjusted_close,
        volume: eod_bar.unadjusted_volume
      )

      # The dividend column reflects the dollar amount of any cash dividend with ex-date on that day.
      # If there is no dividend, this column has value 0.
      if eod_bar.dividend != 0.0
        # per https://www.quandl.com/data/EOD/documentation/methodology: Adjustment Ratio = (Close Price + Dividend Amount) / (Close Price)
        dividend_adjustment_factor = (eod_bar.unadjusted_close + eod_bar.dividend) / eod_bar.unadjusted_close
        CorporateAction.create_cash_dividend(security.id, eod_bar.date, dividend_adjustment_factor)
      end

      # The split_adjustment_factor reflects the ratio of the number of new shares to the number of old shares, assuming a split with ex-date on that day.
      # If there is no split, this column has value 1.
      if eod_bar.split_adjustment_factor != 1.0
        CorporateAction.create_split(security.id, eod_bar.date, eod_bar.split_adjustment_factor)
      end
    end
  end

end
