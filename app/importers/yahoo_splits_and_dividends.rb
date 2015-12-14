require 'date'

class YahooSplitsAndDividendsImporter
  DEFAULT_START_DATE = Date.parse("19500101")

  def import
  end

  def import_records(securities)
    securities.each_with_index do |security, i|
      puts i
      most_recent_record = find_most_recent_record(security)
      start_date = compute_start_date(most_recent_record)
      new_records = download_data(security, start_date)
      save_records(new_records)
      security.reload
    end
  end

  def find_most_recent_record(security)
    most_recent_dividend = security.cash_dividends_dataset.order(:ex_date).reverse.first
    most_recent_split = security.splits_dataset.order(:ex_date).reverse.first

    if most_recent_dividend && most_recent_split
      most_recent_dividend.ex_date.to_i >= most_recent_split.ex_date.to_i ? most_recent_dividend : most_recent_split
    else
      most_recent_dividend || most_recent_split
    end
  end

  def compute_start_date(most_recent_record)
    if most_recent_record
      extract_date(most_recent_record.ex_date) + 1
    else
      DEFAULT_START_DATE
    end
  end

  def extract_date(datestamp)
    Date.parse(datestamp.to_s)
  end

  def download_data(security, start_date)
    end_date = Date.today()

    if start_date <= end_date
      puts "#{security.symbol}:\t#{start_date} to #{end_date}"

      # rows = YahooFinance.retrieve_raw_historical_dividends(security.symbol, start_date, end_date)
      # rows.map do |row|
      #   ex_date, dividend_amount = *yahoo_to_default!(row)
      #   build_record(security, ex_date, dividend_amount)
      # end

      dividends_and_splits = YahooFinance.get_dividends_and_splits(security.symbol, start_date, end_date)
      dividends = dividends_and_splits[:dividends]
      splits = dividends_and_splits[:splits]

      dividends = dividends.map do |row|
        ex_date, dividend_amount = *yahoo_to_default!(row)
        build_dividend(security, ex_date, dividend_amount)
      end

      splits = splits.map do |row|
        ex_date, split_ratio = *yahoo_to_default!(row)
        reformatted_split_ratio = reformat_split_ratio(split_ratio)
        build_split(security, ex_date, reformatted_split_ratio)
      end

      dividends + splits
    else
      []
    end
  end

  # Convert a yahoo format to default format.
  # Converts
  #   [date (yyyy-mm-dd), dividend]
  # to
  #   [date (yyyymmdd), dividend]
  # Note: Modifies the original record/array.
  def yahoo_to_default!(record)
    record[0].gsub!('-','')     # remove hyphens from date
    record
  end

  def build_dividend(security, ex_date, dividend_amount)
    CashDividend.new(:security => security,
                     :ex_date => ex_date.to_i,
                     :number => dividend_amount)
  end

  # converts a split ratio of the form 3:2 (i.e. numerator:denominator) into a decimal of the form 1.5 (i.e. numerator/denominator)
  def reformat_split_ratio(split_ratio)
    numerator, denominator = *(split_ratio.split(":").map(&:strip))
    numerator.to_f / denominator.to_f
  end

  def build_split(security, ex_date, split_ratio)
    Split.new(:security => security,
              :ex_date => ex_date.to_i,
              :number => split_ratio)
  end

  def save_records(records)
    #puts "#{records.count} new records"
    record_being_processed = nil
    records.each do |record|
      record_being_processed = record
      record_being_processed.save
    end
  rescue => e
    puts "Unable to save #{record_being_processed.class.name}: #{record_being_processed.values.to_s}"
    puts ">> #{e.message}"
  end
end


def get_tickers
  ARGV.map {|arg| File.exists?(arg) ? File.readlines(arg).map(&:strip) : arg }.flatten
end

def get_securities
  tickers = get_tickers
  if tickers.empty?
    Stock.us_exchanges.union(Etp.us_exchanges).union(Fund.us_exchanges).union(Index.us_exchanges)
  else
    Security.us_exchanges.where(:symbol => tickers)
  end
end

def main
  Database.connect
  securities = get_securities

  puts "Importing dividends and splits for #{securities.count} securities."
  DividendImporter.new.import_records(securities)
end

main if __FILE__ == $0
