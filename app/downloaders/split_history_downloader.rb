require 'watir-webdriver'

class SplitHistoryDownloader

  BASE_URI = "http://performance.morningstar.com/stock/performance-return.action?p=dividend_split_page&t=<TICKER>&region=USA&culture=en-US"

  def self.extract_split_history_from_morningstar(tickers)
    browser = Watir::Browser.new :firefox
    splits = self.new(browser).extract_split_history_from_morningstar(tickers)
    browser.quit
    splits
  end


  attr_accessor :browser

  def initialize(browser)
    self.browser = browser
  end

  # returns a hash of the form:
  # {ticker1 => [ [date1, split_ratio1], [date2, split_ratio2], ... ],
  #  ticker2 => [ [date3, split_ratio3], [date4, split_ratio4], ... ],
  #  ...}
  # such that each ticker in the argument <tickers> is a key in the return hash.
  def extract_split_history_from_morningstar(tickers)
    #puts "Downloading split history from Morningstar."

    splits = {}
    tickers.each do |ticker|
      #puts "Downloading split history for #{ticker}"

      begin
        # open the split-history page for <ticker>
        uri = BASE_URI.gsub("<TICKER>", ticker)
        browser.goto uri

        # locate the table containing the split data
        split_table = browser.table(:id => "splittable")

        # select the rows in the split data table that contain data (i.e. don't select the first row - the row that acts as an <hr>)
        rows = split_table.tbody.rows(:xpath, "tr[not(@class='hr')]")
        split_history_for_ticker = rows.map do |row|
          cells = row.cells.to_a
          date, split_ratio = cells.map(&:text).map(&:strip)
          # puts "#{date} - #{split_ratio}"
          date = reformat_date(date)
          [date, split_ratio]
        end

        splits[ticker] = split_history_for_ticker
      rescue Exception => e
        puts "Error while processing #{ticker} - #{e.message}"
        next
      end
    end
    splits
  end

  # converts a date in mm/dd/yyyy format to yyyymmdd format
  def reformat_date(date)
    date_fields = date.match(/(\d\d)\/(\d\d)\/(\d\d\d\d)/)
    raise "Date #{date} is not in the expected form: mm/dd/yyyy." unless date_fields
    m, d, y = date_fields.values_at(1, 2, 3)
    "#{y}#{m}#{d}"
  end
end
