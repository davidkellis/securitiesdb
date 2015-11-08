require 'pp'
require_relative '../lib/yahoofinance'
require 'fileutils'

# Convert a yahoo format to default format.
# Converts
#   [date (yyyy-mm-dd), open, high, low, close, volume, adj-close]
# to
#   [date (yyyymmdd), open, high, low, close, volume, adj-close]
# Note: Modifies the original record/array.
def yahoo_to_default!(record)
  record[0].gsub!('-','')     # remove hyphens from date
  record[1] = record[1].to_f.round(2) # open
  record[2] = record[2].to_f.round(2) # high
  record[3] = record[3].to_f.round(2) # low
  record[4] = record[4].to_f.round(2) # close
  record[6] = record[6].to_f.round(2) # adj-close
  record
end

def main
  # Getting the historical quote data as a raw array.
  # The elements of the array are:
  #   [0] - Date
  #   [1] - Open
  #   [2] - High
  #   [3] - Low
  #   [4] - Close
  #   [5] - Volume
  #   [6] - Adjusted Close

  tickers = if ARGV.length == 0
              %w{AAPL F VFINX}
            elsif ARGV.length == 1 && ARGV[0].index(/\.\w+/)   # treat as filename
              File.readlines(ARGV[0]).map{|line| line.strip }
            else
              ARGV
            end

  time_interval_days = 365 * 50   # 50 years

  FileUtils.mkdir('data/stocks') unless File.exists?('data/stocks')

  for ticker in tickers
    puts ticker
    rows = []
    YahooFinance.get_historical_quotes_days(ticker, time_interval_days) do |row|
      rows << "#{yahoo_to_default!(row).join(',')}\n"
    end
    File.open("data/stocks/#{ticker}.csv", "w+") do |f|
      rows.each { |r| f.write r }
    end
  end
end

main if __FILE__ == $0
