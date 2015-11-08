# since I added logic to import_dividends_and_splits.rb to add dividends *and* splits, this script is really no longer necessary unless the Yahoo! data
# is missing splits, in which case I could modify this script to grab for all splits from Morningstar, and then check to see if Morningstar has a record
# of any splits that are missing from the Yahoo!-imported split data

require 'date'
require 'watir-webdriver'
require 'pp'
require_relative '../application'

class SplitImporter
  DEFAULT_START_DATE = Date.parse("19500101")

  def import_records(securities)
    Database.connect

    browser = Watir::Browser.new :firefox
    split_downloader = SplitHistoryDownloader.new(browser)

    for security in securities
      most_recent_record = find_most_recent_record(security)
      start_date = compute_start_date(most_recent_record)
      new_records = download_data(split_downloader, security, start_date)
      save_records(new_records)
      security.reload
    end

    browser.quit
  end

  def find_most_recent_record(security)
    security.splits.first(:order => [:ex_date.desc])
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

  def download_data(split_downloader, security, start_date)
    ticker = security.symbol
    end_date = Date.today()

    if start_date <= end_date
      #puts "#{ticker}:\t#{start_date} to #{end_date}"

      splits = split_downloader.extract_split_history_from_morningstar([ticker])[ticker] || []
      splits = select_splits_within_date_range(splits, start_date, end_date)

      build_records(security, splits)
    else
      []
    end
  end

  # splits is an array of the form: [ [yyyymmdd, 2:1], [yyyymmdd, 3:2], [yyyymmdd, 1.571:1], ... ]
  def select_splits_within_date_range(splits, start_date, end_date)
    splits.select do |pair|
      ex_date = Date.parse(pair.first)
      start_date <= ex_date && ex_date <= end_date
    end
  end

  def build_records(security, splits)
    splits.map do |row|
      ex_date, split_ratio = *row
      reformatted_split_ratio = reformat_split_ratio(split_ratio)
      build_record(security, ex_date, reformatted_split_ratio)
    end
  end

  # converts a split ratio of the form 3:2 (i.e. numerator:denominator) into a decimal of the form 1.5 (i.e. numerator/denominator)
  def reformat_split_ratio(split_ratio)
    numerator, denominator = *(split_ratio.split(":").map(&:strip))
    numerator.to_f / denominator.to_f
  end

  def build_record(security, ex_date, split_ratio)
    Split.new(:security_id => security.id,
              :ex_date => ex_date.to_i,
              :ratio_or_amount => split_ratio.to_s)
  end

  def save_records(records)
    #puts "#{records.count} new records"
    record_being_processed = nil
    records.each do |record|
      record_being_processed = record
      record_being_processed.save
    end
  rescue => e
    puts "Unable to save Split: #{record_being_processed.values.to_s}"
    puts ">> #{e.message}"
  end
end

def get_tickers
  ARGV.map {|arg| File.exists?(arg) ? File.readlines(arg).map(&:strip) : arg }.flatten
end

def get_securities
  tickers = get_tickers
  if tickers.empty?
    Stock.us_stock_exchanges + Etp.us_stock_exchanges + Fund.us_stock_exchanges
  else
    Security.all(:symbol => tickers)
  end
end

def main
  Database.connect
  securities = get_securities

  puts "Importing splits for #{securities.count} securities."
  SplitImporter.new.import_records(securities)
end

main if __FILE__ == $0