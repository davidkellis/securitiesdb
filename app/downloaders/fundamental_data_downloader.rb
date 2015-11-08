require 'pp'
require 'nokogiri'
require 'open-uri'
require 'fileutils'
require 'yaml/store'
require_relative '../protobuf/tradesim.pb'

class DocumentStructureError < StandardError
end

class DownloadError < StandardError
end

class FundamentalDataDownloader
  USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.71"
  BASE_URIS = {
      :annual => "http://www.advfn.com/p.php?pid=financials&btn=start_date&mode=annual_reports&symbol=<TICKER>&start_date=<START_DATE_INDEX>",
      :quarterly => "http://www.advfn.com/p.php?pid=financials&btn=istart_date&mode=quarterly_reports&symbol=<TICKER>&istart_date=<START_DATE_INDEX>"
  }

  INCOME_STATEMENT_HEADER = 'INCOME STATEMENT'
  BALANCE_SHEET_HEADER = 'BALANCE SHEET'
  CASH_FLOW_STATEMENT_HEADERS = ['CASH FLOW STATEMENT', 'CASH-FLOW STATEMENT']

  # returns a map of the form:
  # { TCKR1 => [snapshot1, snapshot2, ...], TKCR2 => [snapshot1, snapshot2, ...], TCKR3 => nil, TCKR4 => [snapshot1, ...], ... }
  def download_financial_reports_within_date_range(tickers, start_date, end_date, report_type = :annual, random_delay_range = 2..10)
    #start_date = convert_date_to_start_of_month(start_date)
    #end_date = convert_date_to_start_of_month(end_date)

    snapshots_per_ticker = tickers.map do |ticker|
      begin
        report_dates_and_indices = get_report_start_dates(ticker, report_type)
        report_dates_within_range = report_dates_and_indices.keys.select{|date| start_date <= date && date <= end_date }
        if report_dates_within_range.empty?
          []
        else
          snapshot_start_index = report_dates_and_indices[report_dates_within_range.first]
          snapshot_end_index = report_dates_and_indices[report_dates_within_range.last]
          download_snapshots_within_date_index_range(ticker, report_type, snapshot_start_index, snapshot_end_index, random_delay_range)
        end
      rescue DocumentStructureError => e
        puts "Error while processing #{ticker} - #{e.message}"
        []
      rescue DownloadError => e
        puts "Error while processing #{ticker} - #{e.message}"
        []
      rescue OpenURI::HTTPError => e
        puts "Error while processing #{ticker} - #{e.message}"
        []
      rescue Exception => e
        puts "Error while processing #{ticker} - #{e.message}"
        puts e.backtrace.join("\n")
        puts "*********** END OF EXCEPTION " * 5
        []
      end
    end

    Hash[ tickers.zip(snapshots_per_ticker) ]
  end

  def convert_date_to_start_of_month(date)
    Date.parse(date.strftime("%Y%m01"))
  end

  # returns a hash of start-date/index pairs of the form: {#<Date: 2001-03-1> => 1, #<Date: 2001-06-1> => 2, ...}
  def get_report_start_dates(ticker, report_type)
    start_date_index = 0

    # open the first/oldest income statement page for <ticker>
    uri = BASE_URIS[report_type].gsub("<TICKER>", ticker).gsub("<START_DATE_INDEX>", start_date_index.to_s)
    document = Nokogiri.HTML(open(uri, "User-Agent" => USER_AGENT))

    raise DownloadError, "No Data Found" if document.xpath('//*[@id="afnmainbodid"]/form/table[3]/tr[1]/td[1]').text.include? "No Data Found"

    # determine the number of snapshots we need to extract
    report_start_date_options = if report_type == :annual
                                  document.css("#start_dateid > option")
                                elsif report_type == :quarterly
                                  document.css("#istart_dateid > option")
                                end

    start_date_index_pairs = report_start_date_options.map do |option_node|
      start_date_prefix = option_node.text   # text of the form: 2005/03
      start_date = Date.parse("#{start_date_prefix}/01")  # date representing start of the month, e.g.: 2005/03/01
      index = option_node["value"].to_i
      [start_date, index]
    end

    Hash[start_date_index_pairs]
  end

  # random_delay_range is a range of timeframes (in seconds) that will be randomly selected from when determining how long to sleep between HTTP requests
  def download_snapshots_within_date_index_range(ticker, report_type, snapshot_start_index, snapshot_end_index, random_delay_range)
    desired_snapshot_count = snapshot_end_index - snapshot_start_index + 1

    # download the first set of snapshots - the snapshots from the first page
    snapshots = download_snapshots(ticker, report_type, snapshot_start_index)
    snapshot_start_index += snapshots.count

    while snapshot_start_index <= snapshot_end_index
      # sleep for a few seconds until we download the next page for this ticker
      sleep(rand(random_delay_range))

      # download the remaining snapshots, from all the rest of the pages
      snapshots_from_page = download_snapshots(ticker, report_type, snapshot_start_index)
      raise DocumentStructureError, "Unable to extract snapshots from page #{snapshot_start_index}." if snapshots_from_page.empty?
      snapshots.concat(snapshots_from_page)
      snapshot_start_index += snapshots_from_page.count
    end

    snapshots.take(desired_snapshot_count)
  end

  # report_type must be either :annual or :quarterly
  def download_financial_reports(tickers, report_type = :annual, directory = ".", random_delay_range = 2..10)
    #puts "Downloading #{report_type.to_s} financial report history from ADVFN."

    tickers.each do |ticker|
      #puts "Processing #{ticker}"

      begin
        snapshots = download_all_snapshots(ticker, report_type, random_delay_range)
        write_snapshots_to_file(directory, ticker, snapshots)
      rescue DocumentStructureError => e
        puts "Error while processing #{ticker} - #{e.message}"
      rescue DownloadError => e
        puts "Error while processing #{ticker} - #{e.message}"
      rescue OpenURI::HTTPError => e
        puts "Error while processing #{ticker} - #{e.message}"
      rescue Exception => e
        puts "Error while processing #{ticker} - #{e.message}"
        puts e.backtrace.join("\n")
        next
      end
    end
  end

  # random_delay_range is a range of timeframes (in seconds) that will be randomly selected from when determining how long to sleep between HTTP requests
  def download_all_snapshots(ticker, report_type, random_delay_range)
    start_date_index = 0

    # sleep for a few seconds until we download the first page for this ticker
    sleep(rand(random_delay_range))

    # open the first/oldest income statement page for <ticker>
    uri = BASE_URIS[report_type].gsub("<TICKER>", ticker).gsub("<START_DATE_INDEX>", start_date_index.to_s)
    document = Nokogiri.HTML(open(uri, "User-Agent" => USER_AGENT))

    raise DownloadError, "No Data Found" if document.xpath('//*[@id="afnmainbodid"]/form/table[3]/tr[1]/td[1]').text.include? "No Data Found"

    # determine the number of snapshots we need to extract
    start_date_range_max_index = if report_type == :annual
                                   document.css("#start_dateid > option").last["value"].to_i
                                 elsif report_type == :quarterly
                                   document.css("#istart_dateid > option").last["value"].to_i
                                 end

    # download the first set of snapshots - the snapshots from the first page
    snapshots = download_snapshots(ticker, report_type, start_date_index, document)
    start_date_index = snapshots.count

    while start_date_index <= start_date_range_max_index
      # sleep for a few seconds until we download the next page for this ticker
      sleep(rand(random_delay_range))

      # download the remaining snapshots, from all the rest of the pages
      snapshots_from_page = download_snapshots(ticker, report_type, start_date_index)
      raise DocumentStructureError, "Unable to extract snapshots from page #{start_date_index}." if snapshots_from_page.empty?
      snapshots.concat(snapshots_from_page)
      start_date_index = snapshots.count
    end

    snapshots
  end

  def download_snapshots(ticker, report_type, start_date_index, document = nil)
    document ||= begin
      uri = BASE_URIS[report_type].gsub("<TICKER>", ticker).gsub("<START_DATE_INDEX>", start_date_index.to_s)
      Nokogiri.HTML(open(uri, "User-Agent" => USER_AGENT))
    end
    extract_snapshots(document)
  end

  def extract_snapshots(document)
    scale_multiplier = extract_scale_multiplier(document)

    # there is only one span element with the style "text-transform: capitalize", so we use that as the anchor for our search
    report_table = document.css('span[style="text-transform: capitalize"] > table:nth-child(2) > tr > td > table')

    # search the report_table for its immediate tr children
    table_rows = report_table > "tr"

    # remove empty rows and rows that only serve as whitespace
    table_rows = table_rows.reject {|row| row.text == '*' }

    # grab second row of table - we want to extract the period end dates
    end_dates_row = table_rows[1]
    end_date_cells = end_dates_row > "td"

    # grab third row of table - report publication dates
    report_publication_dates_row = table_rows[2]
    report_publication_date_cells = report_publication_dates_row > "td"

    # extract report end dates
    report_end_dates = end_date_cells.drop(1).map(&:text).reject{|cell_text| cell_text.empty? }.map{|date_string| convert_quarter_end_date_to_last_day_of_month(date_string) }

    # extract report publication dates
    report_publication_dates = report_publication_date_cells.drop(1).map(&:text).reject{|cell_text| cell_text.empty? }.map{|date_string| convert_string_to_date(date_string) }

    report_publication_dates = report_publication_dates.each_with_index.map{|date, i| date || add_months_to_date(report_end_dates[i], 1) }
    #report_end_dates = report_end_dates.each_with_index.map{|date, i| date || report_publication_dates[i] }

    data_column_count = report_end_dates.count

    # find the index of the rows that begin each of the 3 financial statements: income statement, balance sheet, cash flow statement
    row_index_of_income_statement = table_rows.find_index {|row| row.text == INCOME_STATEMENT_HEADER }
    row_index_of_balance_sheet = table_rows.find_index {|row| row.text == BALANCE_SHEET_HEADER }
    row_index_of_cash_flow_statement = table_rows.find_index {|row| CASH_FLOW_STATEMENT_HEADERS.any? {|header| row.text == header } }

    raise DocumentStructureError, "Unable to locate income statement" unless row_index_of_income_statement
    raise DocumentStructureError, "Unable to locate balance sheet" unless row_index_of_balance_sheet
    raise DocumentStructureError, "Unable to locate cash flow statement" unless row_index_of_cash_flow_statement

    # find the index of the rows that begin the sections following the 3 financial statements
    index_first_header_row_after_income_statement = index_of_row_beginning_next_section(table_rows, row_index_of_income_statement)
    index_first_header_row_after_balance_sheet = index_of_row_beginning_next_section(table_rows, row_index_of_balance_sheet)
    index_first_header_row_after_cash_flow_statement = index_of_row_beginning_next_section(table_rows, row_index_of_cash_flow_statement)

    # count the number of rows in each of the 3 financial statements
    row_count_of_income_statement = index_first_header_row_after_income_statement - row_index_of_income_statement
    row_count_of_balance_sheet = index_first_header_row_after_balance_sheet - row_index_of_balance_sheet
    row_count_of_cash_flow_statement = index_first_header_row_after_cash_flow_statement - row_index_of_cash_flow_statement

    # extract the rows for the income statement, the balance sheet, and the cash flow statement
    income_statement_rows = table_rows[row_index_of_income_statement, row_count_of_income_statement]
    balance_sheet_rows = table_rows[row_index_of_balance_sheet, row_count_of_balance_sheet]
    cash_flow_statement_rows = table_rows[row_index_of_cash_flow_statement, row_count_of_cash_flow_statement]

    # convert the rows of data cells to rows of strings
    income_statement_data_rows = income_statement_rows.map{|row| (row > "td").map(&:text) }.map{|row| cleanup_row_data(row) }
    balance_sheet_data_rows = balance_sheet_rows.map{|row| (row > "td").map(&:text) }.map{|row| cleanup_row_data(row) }
    cash_flow_statement_data_rows = cash_flow_statement_rows.map{|row| (row > "td").map(&:text) }.map{|row| cleanup_row_data(row) }

    # extract the snapshots for each date column
    (1..data_column_count).map do |data_column_index|
      build_snapshot(data_column_index,
                     report_end_dates,
                     report_publication_dates,
                     income_statement_data_rows,
                     balance_sheet_data_rows,
                     cash_flow_statement_data_rows,
                     scale_multiplier)
    end
  end

  def extract_scale_multiplier(document)
    # identify the numeric scale of the amounts - i.e. thousands/millions/billions

    #numeric_scale_text = document.xpath('//*[@id="mainRegForm"]/table[3]/tr[1]/td[1]/center/table/tr/td/table[2]/tr[1]/td[2]').text
    numeric_scale_text = document.xpath('//*[@id="afnmainbodid"]/form/table[3]/tr[1]/td[1]/center/table/tr/td/table[2]/tr[1]/td[2]').text

    #puts "numeric_scale_text=#{numeric_scale_text.inspect}"

    identify_scale_multiplier(numeric_scale_text)
  rescue => e
    raise "Unable to extract the scale multiplier."
  end

  def identify_scale_multiplier(numeric_scale_text)
    case numeric_scale_text
      when /billion/i
        1_000_000_000
      when /million/i
        1_000_000
      when /thousand/i
        1_000
      else
        1
    end
  end

  def convert_quarter_end_date_to_last_day_of_month(date_string)
    year, month = date_string.split("/")
    Date.new(year.to_i, month.to_i, -1).strftime("%Y%m%d") rescue nil
  end

  def convert_string_to_date(date_string)
    Date.parse(date_string).strftime("%Y%m%d") rescue nil
  end

  def add_months_to_date(date_string, months = 1)
    (Date.parse(date_string) >> months).strftime("%Y%m%d")
  end

  # row is an array of strings
  def cleanup_row_data(row)
    row[1..-1] = row[1..-1].map{|cell| cell.gsub(',', '').to_f }
    row
  end

  def build_snapshot(column_index, report_end_dates, report_publication_dates, income_statement_rows, balance_sheet_rows, cash_flow_statement_rows, scale_multiplier)
    income_statement = FinancialStatement.new(:lineItems => [])
    balance_sheet = FinancialStatement.new(:lineItems => [])
    cash_flow_statement = FinancialStatement.new(:lineItems => [])

    income_statement_rows.each do |row|
      attribute = row.first
      income_statement.lineItems << StatementLineItem.new(:type => StatementLineItem::Type::Decimal,
                                                          :attribute => attribute,
                                                          :value => adjust_for_scale_multiplier(attribute, row[column_index], scale_multiplier).to_s)
    end

    balance_sheet_rows.each do |row|
      attribute = row.first
      balance_sheet.lineItems << StatementLineItem.new(:type => StatementLineItem::Type::Decimal,
                                                       :attribute => attribute,
                                                       :value => adjust_for_scale_multiplier(attribute, row[column_index], scale_multiplier).to_s)
    end

    cash_flow_statement_rows.each do |row|
      attribute = row.first
      cash_flow_statement.lineItems << StatementLineItem.new(:type => StatementLineItem::Type::Decimal,
                                                             :attribute => attribute,
                                                             :value => adjust_for_scale_multiplier(attribute, row[column_index], scale_multiplier).to_s)
    end

    {
        :report_end_date => report_end_dates[column_index - 1],
        :report_publication_date => report_publication_dates[column_index - 1],
        :income_statement => income_statement,
        :balance_sheet => balance_sheet,
        :cash_flow_statement => cash_flow_statement
    }
  end

  def adjust_for_scale_multiplier(attribute, value, scale_multiplier)
    if per_share_item? attribute
      value
    else
      if value.nil?
        nil
      else
        value * scale_multiplier
      end
    end
  end

  def per_share_item?(attribute)
    [/EPS/, /per share/i].any? {|regex| regex =~ attribute }
  end

  # find the index of the row that begins the section following the specified row index offset
  def index_of_row_beginning_next_section(table_rows, starting_row_index)
    rows_after_starting_row_index = table_rows.drop(starting_row_index + 1)
    index_of_start_of_next_section = rows_after_starting_row_index.find_index {|row| is_statement_header?(row) }
    index_of_start_of_next_section ? (starting_row_index + 1) + index_of_start_of_next_section : table_rows.count
  end

  def is_statement_header?(row)
    row['bgcolor'] == '#6566a3'
  end

  def write_snapshots_to_file(directory, ticker, snapshots)
    store = YAML::Store.new File.join(directory, "#{ticker}.yml")
    store.transaction do
      store["snapshots"] = snapshots
    end
  end
end

#def main
#  tickers = if ARGV.length == 0
#              %w{AAPL F VFINX}
#            elsif ARGV.length == 1 && File.exist?(ARGV[0])   # treat as filename
#              File.readlines(ARGV[0]).map{|line| line.strip }
#            else
#              ARGV
#            end
#
#  report_type = :quarterly
#
#  directory = "#{report_type}_reports"
#  FileUtils.mkdir(directory) unless File.exists?(directory)
#
#  # grab only tickers that are traded on primary american exchanges - those without an exchange suffix (e.g. ticker.OB)
#  tickers = tickers.select {|ticker| ticker.index(".").nil? }
#  puts "#{tickers.count} symbols"
#
#  catch :ctrl_c do
#    FundamentalDataDownloader.new.download_financial_reports(tickers, report_type, directory)
#  end
#end
#
#trap("SIGINT") { throw :ctrl_c }
#
#main

def test
  downloader = FundamentalDataDownloader.new
  start_date = Date.parse("20050101")
  end_date = Date.parse("20070501")
  report_type = :quarterly

  pp downloader.download_financial_reports_within_date_range(["AAPL"], start_date, end_date, report_type, 0..0)
end

test if __FILE__ == $0
