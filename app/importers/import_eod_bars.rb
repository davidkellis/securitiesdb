require 'date'
require 'pp'
require_relative '../application'

class EodBarImporter
  DEFAULT_START_DATE = Date.parse("19500101")

  def import_eod_bars(securities)
    securities.each_with_index do |security, i|
      puts i
      most_recent_eod_bar = find_most_recent_eod_bar(security)
      start_date = compute_start_date(most_recent_eod_bar)
      eod_bars = download_data(security, start_date)
      save_bars(eod_bars)
      security.reload
    end
  end

  def find_most_recent_eod_bar(security)
    security.eod_bars_dataset.order(:start_time).reverse.first
  end

  def compute_start_date(most_recent_eod_bar)
    if most_recent_eod_bar
      extract_date(most_recent_eod_bar.end_time) + 1
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
      puts "#{ticker}:\t#{start_date} to #{end_date}"

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
    start_time = "#{date}093000".to_i
    end_time = "#{date}160000".to_i
    EodBar.new(:security => security,
               :start_time => start_time,
               :end_time => end_time,
               :open => open,
               :high => high,
               :low => low,
               :close => close,
               :volume => volume.to_i)
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

def get_tickers
  ARGV.map {|arg| File.exists?(arg) ? File.readlines(arg).map(&:strip) : arg }.flatten
end

def get_securities
  tickers = get_tickers
  if tickers.empty?
    # Stock.us_exchanges.union(Etp.us_exchanges).union(Fund.us_exchanges).union(Index.us_exchanges)
    Etp.us_exchanges.union(Fund.us_exchanges).union(Index.us_exchanges)
  else
    Security.us_exchanges.where(:symbol => tickers)
  end
end

def main
  Database.connect
  securities = get_securities
  
  puts "Importing eod bars for #{securities.count} securities."
  EodBarImporter.new.import_eod_bars(securities)
end

main if __FILE__ == $0