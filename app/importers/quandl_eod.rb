require 'csv'
require 'zip'

class QuandlEodImporter
  def initialize(quandl_eod_client)
    @client = quandl_eod_client
  end

  def import
    eod_bars = @client.all_eod_bars
    import_eod_bars_splits_and_dividends(eod_bars)
  end

  private

  def log(msg)
    Application.logger.info("#{Time.now} - #{msg}")
  end

  def error(msg)
    Application.logger.error("#{Time.now} - #{msg}")
  end

  def import_eod_bars_splits_and_dividends(all_eod_bars)
    all_eod_bars.each do |symbol, eod_bars|
      # eod_bars is an array of QuandlEod::EodBar objects; each has the following fields:
      #   date,   # this is an integer of the form yyyymmdd
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
      if !eod_bars.empty?
        securities = FindSecurity.us_stocks.all(symbol, eod_bars.first.date)
        case securities.count
        when 0
          log "Security symbol '#{symbol}' not found in any US exchange."
        when 1
          security = securities.first
          most_recent_eod_bar = security.eod_bars_dataset.reverse_order(:date).first

          if most_recent_eod_bar
            import_missing_eod_bars_splits_and_dividends(security, eod_bars.select {|eod_bar| eod_bar.date > most_recent_eod_bar.date })
          else
            import_missing_eod_bars_splits_and_dividends(security, eod_bars)
          end
        else
          security_references = securities.map(&:to_hash)
          error "Error: Security symbol '#{symbol}' identifies multiple securities:\n#{security_references.join("\n")}."
        end
      end
    end
  end

  # eod_bars is an array of QuandlEod::EodBar objects
  def import_missing_eod_bars_splits_and_dividends(security, eod_bars)
    log "Importing #{eod_bars.count} EOD bars from Quandl EOD database for security \"#{security.name}\"."

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
        CorporateAction.create_cash_dividend(security.id, eod_bar.date, nil, nil, nil, dividend_adjustment_factor)
      end

      # The split_adjustment_factor reflects the ratio of the number of new shares to the number of old shares, assuming a split with ex-date on that day.
      # If there is no split, this column has value 1.
      if eod_bar.split_adjustment_factor != 1.0
        CorporateAction.create_split(security.id, eod_bar.date, eod_bar.split_adjustment_factor)
      end
    end
  end

end
