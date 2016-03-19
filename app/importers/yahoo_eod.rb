require 'date'

class YahooEodImporter
  DEFAULT_START_DATE = Date.parse("19500101")

  def initialize
    @find_security = FindSecurity.new(Exchange.us_composite, Exchange.us_stock_exchanges.to_a, nil)
  end

  def import
    prior_business_day = Date.prior_business_day(Date.today).strftime("%Y%m%d").to_i
    # look up securities whose most recent EOD bar is dated more than a day ago
    # todo, fix this query
    securities = Security.
                   us_exchanges.
                   join(:eod_bars, :security_id => :id).
                   where(
                     security_type: [SecurityType.stock, SecurityType.etp] + SecurityType.funds,
                   ).
                   where {
                     (bbgid_composite =~ nil) |     # search for securities that have no composite BBGID - these securities are traded only on a single local exchange
                     ((bbgid_composite !~ nil) & (exchange_id =~ Exchange.us_composite.id))
                   }.
                   group(:securities__id).
                   having { max(Sequel.qualify(:eod_bars, :date)) < prior_business_day }
    import_eod_bars(securities)
  end

  private

  def log(msg)
    Application.logger.info("#{Time.now} - #{msg}")
  end

  def import_eod_bars(securities)
    securities.each_with_index do |security, i|
      most_recent_eod_bar = find_most_recent_eod_bar(security)
      start_date = compute_start_date(most_recent_eod_bar)
      eod_bars = download_data(security, start_date)
      save_bars(eod_bars)
      security.reload
    end
  end

  def find_most_recent_eod_bar(security)
    security.eod_bars_dataset.order(:date).reverse.first
  end

  def compute_start_date(most_recent_eod_bar)
    if most_recent_eod_bar
      Date.datestamp_to_date(most_recent_eod_bar.date) + 1
    else
      DEFAULT_START_DATE
    end
  end

  def extract_date(timestamp)
    Date.parse(timestamp.to_s[0...8])
  end

  def download_data(security, start_date)
    # Getting the historical quote data as a raw array.
    # The elements of the array are:
    #   [0] - Date
    #   [1] - Open
    #   [2] - High
    #   [3] - Low
    #   [4] - Close
    #   [5] - Volume
    #   [6] - Adjusted Close

    ticker = security.symbol
    end_date = Date.today()

    records = []
    if start_date <= end_date
      log "#{ticker}:\t#{start_date} to #{end_date}"

      YahooFinance.get_historical_quotes(ticker, start_date, end_date) do |row|
        date, open, high, low, close, volume = *yahoo_to_default!(row)
        records << build_eod_record(security, date, open, high, low, close, volume)
      end
    end
    records
  end

  # Convert a yahoo format to default format.
  # Converts
  #   [date (yyyy-mm-dd), open, high, low, close, volume, adj-close]
  # to
  #   [date (yyyymmdd), open, high, low, close, volume]
  # Note: Modifies the original record/array.
  def yahoo_to_default!(record)
    record[0].gsub!('-','')     # remove hyphens from date
    record.take(6)
  end

  def build_eod_record(security, date, open, high, low, close, volume)
    EodBar.new(security: security,
               date: start_time,
               open: open,
               high: high,
               low: low,
               close: close,
               volume: volume.to_i)
  end

  def save_bars(eod_bars)
    #puts "#{eod_bars.count} new records"
    bar_being_processed = nil
    eod_bars.each do |eod_bar|
      bar_being_processed = eod_bar
      bar_being_processed.save
    end
  rescue => e
    puts "Unable to save EOD bar: #{bar_being_processed.values.to_s}"
    puts ">> #{e.message}"
  end
end
