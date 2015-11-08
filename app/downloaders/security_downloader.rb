require 'csv'
require 'date'
require 'net/http'
require 'securerandom'
require 'spreadsheet'
require 'zip'
require 'stringio'
require 'pp'
require_relative '../application'

# class CsiDataSecurityDownloader
#
#   Security = Struct.new(:symbol, :name, :exchange, :is_active, :start_date, :end_date)
#
#   AMEX_URL = "http://www.csidata.com/factsheets.php?type=stock&format=csv&exchangeid=80"
#   NASDAQ_URL = "http://www.csidata.com/factsheets.php?type=stock&format=csv&exchangeid=88"
#   NYSE_URL = "http://www.csidata.com/factsheets.php?type=stock&format=csv&exchangeid=79"
#   STOCK_URLS = [AMEX_URL, NASDAQ_URL, NYSE_URL]
#
#   MUTUAL_FUND_URL = "http://www.csidata.com/factsheets.php?type=stock&format=csv&exchangeid=85"
#
#   STOCK_INDICES_URL = "http://www.csidata.com/factsheets.php?type=stock&format=csv&exchangeid=81"
#
#   def exchanges
#     exchange_names = get_stocks.map(&:exchange).uniq
#     exchange_names.each {|exchange_name| Exchange.first_or_create(:name => exchange_name) }
#   end
#
#   def get_stocks
#     STOCK_URLS.map {|url| get_securities(url) }.flatten
#   end
#
#   def get_mutual_funds
#     get_securities(MUTUAL_FUND_URL)
#   end
#
#   def get_stock_indices
#     get_securities(STOCK_INDICES_URL)
#   end
#
#   def get_securities(url)
#     # open the stock data factsheet at CSI data
#     csv_file = Net::HTTP.get(URI(url))
#     rows = CSV.parse(csv_file)
#     rows.drop(1).map {|row| convert_row_to_security_row(row) }
#   rescue Exception => e
#     puts "Error while processing #{url}: #{e.message}"
#     nil
#   end
#
#   def convert_row_to_security_row(row)
#     symbol = row[1]
#     name = row[2]
#     exchange = row[3]
#     is_active = row[4] == "1" ? true : false
#     start_date = reformat_date(row[5])
#     end_date = reformat_date(row[6])
#     Security.new(symbol, name, exchange, is_active, start_date, end_date)
#   end
#
#   # converts a date in yyyy-mm-dd format to yyyymmdd format
#   def reformat_date(date)
#     date.gsub("-", "") if date
#   end
#
# end

class BloombergSecurityDownloader

  SecurityRecord = Struct.new(:figi, :bb_gcid, :name, :symbol, :exchange_label)

  PRICING_SOURCES_URL = "http://bsym.bloomberg.com/sym/pages/pricing_source.xls"
  PRICING_SOURCES_HEADER_ROW = "Yellow Key Database,Pricing Source,Description"

  US_PRICING_SOURCE_LABELS = {
    amex: "UA",
    nasdaq: "UQ",
    nyse: "UN",
    otc: "UU",
    non_otc_bulletin_board: "UV",
    us_composite: "US"
  }

  PREDEFINED_FILES = {
    common_stock: "http://bdn-ak.bloomberg.com/precanned/Equity_Common_Stock_<DATE>.txt.zip",
    equity_index: "http://bdn-ak.bloomberg.com/precanned/Index_Equity_Index_<DATE>.txt.zip",
    etp: "http://bdn-ak.bloomberg.com/precanned/Equity_ETP_<DATE>.txt.zip",
    fund_of_funds: "http://bdn-ak.bloomberg.com/precanned/Equity_Fund_of_Funds_<DATE>.txt.zip",
    mutual_fund: "http://bdn-ak.bloomberg.com/precanned/Equity_Mutual_Fund_<DATE>.txt.zip",
    open_end_fund: "http://bdn-ak.bloomberg.com/precanned/Equity_Open-End_Fund_<DATE>.txt.zip"
  }
  SECURITIES_LIST_HEADER_ROW = ["NAME",
                                "ID_BB_SEC_NUM_DES",
                                "FEED_SOURCE",
                                "ID_BB_SEC_NUM_SRC",
                                "ID_BB_UNIQUE",
                                "SECURITY_TYP",
                                "MARKET_SECTOR_DES",
                                "ID_BB_GLOBAL",
                                "COMPOSITE_ID_BB_GLOBAL",
                                "FEED_EID1",
                                "FEED_EID2",
                                "FEED_EID3",
                                "FEED_EID4",
                                "FEED_DELAYED_EID1",
                                "Subscription String 1",
                                "Subscription String 2",
                                "Subscription String 3"].join("|")
  SECURITIES_LIST_CELL_COUNT = 17

  def initialize
    @exchange_memo = {}
    @today = Date.today
    @today = @today - (@today.cwday - 5) if @today.cwday == 6 || @today.cwday == 7    # make @today be most recent friday if today is sat/sun
  end

  def exchanges
    excel_file_contents = Net::HTTP.get(URI(PRICING_SOURCES_URL))
    #filename = "exchanges-#{SecureRandom.uuid}.xls"
    filename = File.basename(PRICING_SOURCES_URL)
    File.write(filename, excel_file_contents)
    exchanges = Spreadsheet.open(filename) do |book|
      # row headers are: Yellow Key Database,Pricing Source,Description
      # each row looks like this:
      # Equity,A0,Asset Match MTF
      rows = book.worksheet('Sheet1')
      if rows.first.join(",") == PRICING_SOURCES_HEADER_ROW
        rows.drop(1).map do |row|
          build_exchange(row)
        end
      else
        raise "The pricing sources spreadsheet, #{PRICING_SOURCES_URL}, doesn't conform to the expected row structure of: #{PRICING_SOURCES_HEADER_ROW}."
      end
    end
    File.delete(filename)
    exchanges
  end

  # row is of the form: Yellow Key Database,Pricing Source,Description ; e.g. Equity,A0,Asset Match MTF
  def build_exchange(pricing_source_row)
    Exchange.new(label: pricing_source_row[1], name: pricing_source_row[2])
  end

  def stocks
    #get_securities(PREDEFINED_FILES[:common_stock], Stock)
    enum_for(:get_securities, PREDEFINED_FILES[:common_stock])
  end

  def etps
    #get_securities(PREDEFINED_FILES[:etp], Etp)
    enum_for(:get_securities, PREDEFINED_FILES[:etp])
  end

  def fund_of_funds
    enum_for(:get_securities, PREDEFINED_FILES[:fund_of_funds])
  end

  def mutual_funds
    enum_for(:get_securities, PREDEFINED_FILES[:mutual_fund])
  end

  def open_end_funds
    enum_for(:get_securities, PREDEFINED_FILES[:open_end_fund])
  end

  # includes all fund_of_funds, mutual_funds, and open_end_funds
  def funds
    Enumerator.new do |y|
      fund_of_funds.each {|fund| y << fund }
      mutual_funds.each {|fund| y << fund }
      open_end_funds.each {|fund| y << fund }
    end
  end

  def indices
    #get_securities(PREDEFINED_FILES[:equity_index], Index)
    enum_for(:get_securities, PREDEFINED_FILES[:equity_index])
  end

  # expects a block to be given
  def get_securities(url_template, &blk)
    url = url_template.gsub("<DATE>", @today.strftime("%Y%m%d"))
    zip_file_contents = Net::HTTP.get(URI(url))
    #temp_filename = "securities-#{SecureRandom.uuid}.txt.zip"
    temp_filename = File.basename(url)

    # write the zip file to disk
    File.write(temp_filename, zip_file_contents)

    # extract the contents from the zip file
    txt_files = Zip::File.open(temp_filename) do |zip_file|
      zip_file.map do |entry|
        entry.get_input_stream.read
      end
    end

    # delete the zip file from disk
    File.delete(temp_filename)

    txt_files.each do |txt_file_contents|
      lines = txt_file_contents.lines
      if lines.first.strip == SECURITIES_LIST_HEADER_ROW      # does file conform to expected structure?
        lines.drop(1).each do |line|
          line.strip!
          if !line.empty? && !line.start_with?("#")     # ignore empty lines and comment lines
            row = line.split("|", -1)
            if row.size == SECURITIES_LIST_CELL_COUNT
              blk.call(convert_row_to_security(row))
            else
              puts "Cannot parse row: #{line}"
            end
          end
        end
      else
        raise "The securities list doesn't conform to the expected row structure of: #{SECURITIES_LIST_HEADER_ROW}."
      end
    end
    nil
  rescue Exception => e
    puts "Error while processing #{url}: #{e.message}"
    puts e.backtrace.join("\n")
    nil
  end

  # Each row is of them form:
  # ["NAME",                    # name
  #  "ID_BB_SEC_NUM_DES",       # ticker
  #  "FEED_SOURCE",             # pricing source === exchange label
  #  "ID_BB_SEC_NUM_SRC",       # BSID (=== id of pricing source)
  #  "ID_BB_UNIQUE",            # unique id
  #  "SECURITY_TYP",            # security type
  #  "MARKET_SECTOR_DES",       # market sector (e.g. Equity/Bond/etc.)
  #  "ID_BB_GLOBAL",            # bloomberg global id (unique per security per exchange)
  #  "COMPOSITE_ID_BB_GLOBAL",  # bloomberg global composite id (unique per security - shared across exchanges)
  #  "FEED_EID1",
  #  "FEED_EID2",
  #  "FEED_EID3",
  #  "FEED_EID4",
  #  "FEED_DELAYED_EID1",
  #  "Subscription String 1",
  #  "Subscription String 2",
  #  "Subscription String 3"]
  # See http://bsym.bloomberg.com/sym/pages/bsym-whitepaper.pdf for explanation of each field
  def convert_row_to_security(row)
    name = row[0]
    ticker = row[1]
    exchange_label = row[2]
    figi = row[7]
    bb_gcid = row[8]

    SecurityRecord.new(
      figi,
      bb_gcid,
      name,
      ticker,
      exchange_label
    )
  end
end

def main
  Database.connect
  #pp SecurityDownloader.new.get_securities(SecurityDownloader::AMEX_URL)
  downloader = BloombergSecurityDownloader.new
  stocks = downloader.stocks
  pp stocks.count
  exchanges = [Exchange.amex, Exchange.nasdaq, Exchange.nyse]
  pp stocks.select{|s| exchanges.include? s.exchange }.count
  #pp exchanges.first.values
end

main if __FILE__ == $0
