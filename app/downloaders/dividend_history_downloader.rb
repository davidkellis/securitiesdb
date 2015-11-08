require 'pp'
require_relative 'yahoofinance'
require 'fileutils'

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

def main
  tickers = if ARGV.length == 0
              %w{AAPL F VFINX}
            elsif ARGV.length == 1 && ARGV[0].index(/\.\w+/)   # treat as filename
              File.readlines(ARGV[0]).map{|line| line.strip }
            else
              ARGV
            end

  time_interval_days = 365 * 50   # 50 years

  FileUtils.mkdir('dividends') unless File.exists?('dividends')

  for ticker in tickers
    puts ticker
    rows = []
    YahooFinance::get_historical_dividends_days(ticker, time_interval_days) do |row|
      rows << "#{yahoo_to_default!(row).join(',')}\n"
    end
    File.open("dividends/#{ticker}.csv", "w+") do |f|
      rows.each { |r| f.write r }
    end
  end
end

main