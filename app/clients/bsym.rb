# This library makes the various contents of the Bloomberg Open Symbology website available through a nice API.

require 'csv'
require 'date'
require 'nokogiri'
require 'open-uri'
require 'net/http'
require 'simple-spreadsheet'
require 'zip'
require 'stringio'
require 'pp'
require_relative "browser"

module Bsym

  # the BsymFields were taken from the table of fields documented in the Open Fields tab of http://bsym.bloomberg.com/sym/ as of Nov. 6, 2015.
  BsymField = Struct.new(:id, :mnemonic, :description)
  BsymFields = [
    BsymField.new("DS001", "TICKER",                         "Ticker"),
    BsymField.new("DS002", "NAME",                           "Name"),
    BsymField.new("DS122", "MARKET_SECTOR_DES",              "Market Sector Description"),
    BsymField.new("DS144", "EXCH_CODE",                      "Exchange Code"),
    BsymField.new("DS213", "SECURITY_TYP",                   "Security Type"),
    BsymField.new("DX282", "FEED_SOURCE",                    "Feed Source B-Pipe"),
    BsymField.new("DX283", "FEED_EID1",                      "Feed Entitlement ID1"),
    BsymField.new("DX284", "FEED_EID2",                      "Feed Entitlement ID2"),
    BsymField.new("DX285", "FEED_EID3",                      "Feed Entitlement ID3"),
    BsymField.new("DX286", "FEED_EID4",                      "Feed Entitlement ID4"),
    BsymField.new("DY003", "ID_BB_SEC_NUM_DES",              "Bloomberg Security ID Number Description"),
    BsymField.new("DY740", "FEED_DELAYED_EID1",              "Delayed Entitlement ID 1"),
    BsymField.new("ID059", "ID_BB_UNIQUE",                   "Unique Bloomberg Identifier"),
    BsymField.new("ID122", "ID_BB_SEC_NUM_SRC",              "Bloomberg Security ID Number With Source"),
    BsymField.new("ID135", "ID_BB_GLOBAL",                   "Bloomberg Global Identifier"),
    BsymField.new("ID145", "COMPOSITE_ID_BB_GLOBAL",         "Composite Bloomberg Global Identifier"),
    BsymField.new("DS010", "NAME_CHINESE_SIMPLIFIED",        "Name - Chinese Simplified"),
    BsymField.new("DS135", "MARKET_SECTOR",                  "Market Sector Number"),
    BsymField.new("DS156", "SECURITY_DES",                   "Security Description"),
    BsymField.new("DS242", "NAME_KANJI",                     "Japanese Name"),
    BsymField.new("DS674", "SECURITY_TYP2",                  "Security Type 2"),
    BsymField.new("DX312", "SECURITY_SHORT_DES",             "Security Short Description"),
    BsymField.new("ID008", "FEED_ID_BB_SECURITY",            "Trading Session B-Pipe Security ID"),
    BsymField.new("ID114", "ID_BB_CONNECT",                  "Security Node Id"),
    BsymField.new("ID121", "ID_BB_SEC_NUM",                  "Bloomberg Security ID Number"),
    BsymField.new("ID124", "UNIQUE_ID_FUT_OPT",              "Unique Bloomberg ID for Future Option"),
    BsymField.new("ID129", "ID_BPIPE_SEC_NUM_SRC_COMP",      "Composite B-Pipe Security ID Number"),
    BsymField.new("ID236", "ID_BB_GLOBAL_SHARE_CLASS_LEVEL", "Security Share Class Level Bloomberg Global Id")
  ].map {|f| [f.mnemonic, f] }.to_h

  PricingSource = Struct.new(:yellow_key_database, :label, :description)

  SecurityType = Struct.new(:market_sector, :security_type)

  ExchangeCode = Struct.new(:composite_exchange_code, :composite_exchange_name, :local_exchange_code, :local_exchange_name)

  # Security fields (for more info about what each field means, see http://bsym.bloomberg.com/sym/pages/bsym-whitepaper.pdf):
  # "NAME",                    # name
  # "ID_BB_SEC_NUM_DES",       # ticker
  # "FEED_SOURCE",             # pricing source === exchange label
  # "ID_BB_SEC_NUM_SRC",       # irrelevant - BSID (Bloomberg Security ID Number with Source) - e.g. 1095270768082
  # "ID_BB_UNIQUE",            # irrelevant - unique id - e.g. IX26248014-0
  # "SECURITY_TYP",            # security type - corresponds to the Bloomberg Yellow Key - the market sector and the security type pair corresponds to the pairs listed at <SECURITY_TYPES_URL>
  # "MARKET_SECTOR_DES",       # market sector (e.g. Equity/Bond/etc.) - the market sector and the security type pair corresponds to the pairs listed at <SECURITY_TYPES_URL>
  # "ID_BB_GLOBAL",            # FIGI (formerly BBGID) - bloomberg global id (unique per security per exchange) - e.g. BBG009T64180
  # "COMPOSITE_ID_BB_GLOBAL",  # bloomberg global composite id (unique per security - shared across exchanges) - **not always defined** - e.g. BBG000GGBTC7
  # "FEED_EID1",
  # "FEED_EID2",
  # "FEED_EID3",
  # "FEED_EID4",
  # "FEED_DELAYED_EID1",
  # "Subscription String 1",
  # "Subscription String 2",
  # "Subscription String 3"
  Security = Struct.new(:name, :ticker, :pricing_source, :bsid, :unique_id, :security_type, :market_sector, :figi, :composite_bbgid)

  class Client
    BASE_URL = "http://bsym.bloomberg.com/sym/"
    USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36"

    # found at https://cdp.bloomberg.com/bsyms/new
    # -> Show File Submission instructions
    # -> link to "Listing of exchange codes"
    # -> https://cdp.bloomberg.com/docs/exchcodemappings.xlsx
    EXCHANGE_CODE_MAPPINGS_URL = "https://cdp.bloomberg.com/docs/exchcodemappings.xlsx"
    EXCHANGE_CODE_MAPPINGS_HEADER_ROW = "Composite Exchange Code,Composite Exchange Name,Local Exchange Code,Local Exchange Name"

    PRICING_SOURCES_URL = "http://bsym.bloomberg.com/sym/pages/pricing_source.xls"
    PRICING_SOURCES_HEADER_ROW = "Yellow Key Database,Pricing Source,Description"

    SECURITY_TYPES_URL = "http://bsym.bloomberg.com/sym/pages/security_type.csv"
    SECURITY_TYPES_HEADER_ROW = "Market Sector,Security Type"

    # for more info about what each field means, see http://bsym.bloomberg.com/sym/pages/bsym-whitepaper.pdf
    SECURITIES_LIST_HEADER_ROW = ["NAME",                     # name
                                  "ID_BB_SEC_NUM_DES",        # ticker
                                  "FEED_SOURCE",              # pricing source === exchange label
                                  "ID_BB_SEC_NUM_SRC",        # irrelevant - BSID (Bloomberg Security ID Number with Source) - e.g. 1095270768082
                                  "ID_BB_UNIQUE",             # irrelevant - unique id - e.g. IX26248014-0
                                  "SECURITY_TYP",             # security type - corresponds to the Bloomberg Yellow Key - the market sector and the security type pair corresponds to the pairs listed at <SECURITY_TYPES_URL>
                                  "MARKET_SECTOR_DES",        # market sector (e.g. Equity/Bond/etc.) - the market sector and the security type pair corresponds to the pairs listed at <SECURITY_TYPES_URL>
                                  "ID_BB_GLOBAL",             # FIGI (formerly BBGID) - bloomberg global id (unique per security per exchange) - e.g. BBG009T64180
                                  "COMPOSITE_ID_BB_GLOBAL",   # bloomberg global composite id (unique per security - shared across exchanges) - **not always defined** - e.g. BBG000GGBTC7
                                  "FEED_EID1",
                                  "FEED_EID2",
                                  "FEED_EID3",
                                  "FEED_EID4",
                                  "FEED_DELAYED_EID1",
                                  "Subscription String 1",
                                  "Subscription String 2",
                                  "Subscription String 3"].join("|")
    SECURITIES_LIST_CELL_COUNT = 17

    attr_accessor :logger

    def initialize(logger)
      @logger = logger
    end

    def log(msg)
      Application.logger.info("#{Time.now} - #{msg}")
    end

    # the BsymFields were taken from the table of fields documented in the Open Fields tab of http://bsym.bloomberg.com/sym/ as of Nov. 6, 2015.
    def open_fields
      BsymFields
    end

    # returns the contents of http://bsym.bloomberg.com/sym/pages/pricing_source.xls as an array of PricingSource objects, of the form:
    # [
    #   #<struct PricingSource yellow_key_database="Equity", label="A0", description="Asset Match MTF">,
    #   #<struct PricingSource yellow_key_database="Equity, Index", label="AA", description="Athens Exchange Alternative Market">,
    #   #<struct PricingSource yellow_key_database="Corporate, Government, Preferred", label="AABA", description="RBS HK & KR Government Bonds">,
    #   ...
    # ]
    def pricing_sources
      excel_file_contents = Net::HTTP.get(URI(PRICING_SOURCES_URL))
      filename = File.basename(PRICING_SOURCES_URL)
      File.write(filename, excel_file_contents)

      # row headers are: Yellow Key Database,Pricing Source,Description
      # each row looks like this:
      # Equity,A0,Asset Match MTF
      begin
        s = SimpleSpreadsheet::Workbook.read(filename)
        s.selected_sheet = s.sheets.first
        rows = spreadsheet_rows(s)
        if rows.first.join(",") == PRICING_SOURCES_HEADER_ROW
          rows.drop(1).map do |row|
            PricingSource.new(row[0], row[1], row[2])
          end
        else
          raise "The pricing sources spreadsheet, #{PRICING_SOURCES_URL}, doesn't conform to the expected row structure of: #{PRICING_SOURCES_HEADER_ROW}."
        end
      ensure
        File.delete(filename)
      end
    end

    # returns the contents of https://cdp.bloomberg.com/docs/exchcodemappings.xlsx as an array of ExchangeCode objects, of the form:
    # [
    #   #<struct Bsym::ExchangeCode composite_exchange_code="Z1", composite_exchange_name="AIAF", local_exchange_code="Z1", local_exchange_name="AIAF">,
    #   ...
    #   #<struct Bsym::ExchangeCode composite_exchange_code="US", composite_exchange_name="United States", local_exchange_code="PQ", local_exchange_name="OTC Markets">,
    #   #<struct Bsym::ExchangeCode composite_exchange_code="US", composite_exchange_name="United States", local_exchange_code="UA", local_exchange_name="NYSE Amex">,
    #   ...
    # ]
    def exchange_codes
      excel_file_contents = Net::HTTP.get(URI(EXCHANGE_CODE_MAPPINGS_URL))
      filename = File.basename(EXCHANGE_CODE_MAPPINGS_URL)
      File.write(filename, excel_file_contents)

      # row headers are: Composite Exchange Code, Composite Exchange Name, Local Exchange Code, Local Exchange Name
      # each row looks like this:
      # US,United States,UA,NYSE Amex
      begin
        s = SimpleSpreadsheet::Workbook.read(filename)
        s.selected_sheet = s.sheets.first
        rows = spreadsheet_rows(s)
        if rows.first.join(",") == EXCHANGE_CODE_MAPPINGS_HEADER_ROW
          rows.drop(1).map do |row|
            ExchangeCode.new(*row)
          end
        else
          raise "The exchange codes spreadsheet, #{EXCHANGE_CODE_MAPPINGS_URL}, doesn't conform to the expected row structure of: #{EXCHANGE_CODE_MAPPINGS_HEADER_ROW}."
        end
      ensure
        File.delete(filename)
      end
    end

    # returns the contents of http://bsym.bloomberg.com/sym/pages/security_type.csv as an array of SecurityType objects, of the form:
    # [
    #   #<struct SecurityType market_sector="Comdty", security_type="Calendar Spread Option">,
    #   #<struct SecurityType market_sector="Comdty", security_type="Financial commodity future.">,
    #   #<struct SecurityType market_sector="Comdty", security_type="Financial commodity generic.">,
    #   ...
    # ]
    # A security type is essentially equivalent to an asset class category.
    def security_types
      csv_contents = Net::HTTP.get(URI(SECURITY_TYPES_URL))
      rows = CSV.parse(csv_contents, headers: false, return_headers: false, skip_lines: /^(\s*,\s*)*$/)
      if rows.first.join(",") == SECURITY_TYPES_HEADER_ROW
        rows.drop(1).map {|row| SecurityType.new(*row) }
      else
        raise "The security types spreadsheet, #{SECURITY_TYPES_URL}, doesn't conform to the expected row structure of: #{SECURITY_TYPES_HEADER_ROW}."
      end
    end

    # returns a Hash of the form:
    # {
    #   "Commodity/Calendar Spread Option" => "http://...",
    #   "Commodity/Financial commodity future" => "http://...",
    #   ...,
    #   "Equity/Common Stock" => "http://bdn-ak.bloomberg.com/precanned/Equity_Common_Stock_20151106.txt.zip",
    #   "Equity/Conv Bond" => "http://...",
    #   ...
    # }
    # The keys of this hash closely, but not perfectly, correspond to the security types returned from #security_types
    def predefined_files(reload = false)
      @predefined_files = if @predefined_files.nil? || reload
        b = Browser.open(Dir.pwd)

        file_map = {}

        begin
          b.goto(BASE_URL)

          predefined_files_expander_menu = b.span :text => 'Predefined Files'
          predefined_files_expander_menu.click

          # locate the tree-menu sections
          category_tree_menus = b.divs(:css => "#exeSearches .x-grid-group")
          category_tree_menus.each do |category_tree_menu|
            # locate the tree-menu header div - e.g. "Commodity (7 files)", "Corporate (96 files)", ...
            tree_menu_heading = category_tree_menu.div(:css => ".x-grid-group-hd")
            tree_menu_heading.click

            # extract category name from header div - e.g. extract "Commodity" from "Commodity (7 files)"
            m = /(.*?)\s+\(\d+ files\)/.match(tree_menu_heading.text)
            category_name = m[1]

            # identify the sub-category names from the sub-menu row divs "within" the sub-tree rooted at <category_tree_menu>
            sub_category_filename_links = category_tree_menu.links(:css => ".x-grid-group-body .x-grid3-td-filename .x-grid3-col-filename > a")
            sub_category_filename_links.each do |link|
              sub_category_name = link.text

              qualified_category_name = "#{category_name}/#{sub_category_name}"

              qualified_category_url = link.href

              file_map[qualified_category_name] = qualified_category_url
            end
          end

        ensure
          b.quit
        end

        file_map
      else
        @predefined_files
      end
    end


    # Securities

    def all_securities
      predefined_files.values.reduce([]) {|memo, url| memo + get_securities(url).to_a }
    end

    def stocks
      get_securities_from_predefined_file("Equity/Common Stock")
    end

    def etps
      get_securities_from_predefined_file("Equity/ETP")
    end

    def fund_of_funds
      get_securities_from_predefined_file("Equity/Fund of Funds")
    end

    def mutual_funds
      get_securities_from_predefined_file("Equity/Mutual Fund")
    end

    def open_end_funds
      get_securities_from_predefined_file("Equity/Open-End Fund")
    end

    # includes all fund_of_funds, mutual_funds, and open_end_funds
    def funds
      fund_of_funds.to_a +
      mutual_funds.to_a +
      open_end_funds.to_a
    end

    def indices
      get_securities_from_predefined_file("Index/Equity Index")
    end

    # predefined_file_security_type is one of the keys from the hash returned by #predefined_files
    def get_securities_from_predefined_file(predefined_file_security_type, &blk)
      url = predefined_files[predefined_file_security_type]
      get_securities(url, &blk)
    end

    # url is one of the urls found by the #predefined_files method
    def get_securities(url)
      if block_given?
        zip_file_contents = Net::HTTP.get(URI(url))
        #temp_filename = "securities-#{SecureRandom.uuid}.txt.zip"
        temp_filename = File.basename(url)

        # write the zip file to disk
        File.write(temp_filename, zip_file_contents)

        # extract the contents from the zip file
        txt_files = begin
          Zip::File.open(temp_filename) do |zip_file|
            zip_file.map do |entry|
              entry.get_input_stream.read
            end
          end
        ensure
          # delete the zip file from disk
          File.delete(temp_filename)
        end

        txt_files.each_with_index do |txt_file_contents, txt_file_index|
          lines = txt_file_contents.lines
          if lines.first.strip == SECURITIES_LIST_HEADER_ROW      # does file conform to expected structure?
            lines.drop(1).each do |line|
              line.strip!
              if !line.empty? && !line.start_with?("#")     # ignore empty lines and comment lines
                row = line.split("|", -1)
                if row.size == SECURITIES_LIST_CELL_COUNT
                  yield convert_row_to_security(row)
                else
                  log "Cannot parse row in #{url}, zip entry index #{txt_file_index}: #{line}"
                end
              end
            end
          else
            raise "The securities list in #{url}, zip entry index #{txt_file_index}, doesn't conform to the expected row structure of: #{SECURITIES_LIST_HEADER_ROW}."
          end
        end
        nil
      else
        enum_for(:get_securities, url)
      end
    end

    # Each row in the predefined files containing lists of securities is of them form:
    # ["NAME",                    # name
    #  "ID_BB_SEC_NUM_DES",       # ticker
    #  "FEED_SOURCE",             # pricing source === exchange label
    #  "ID_BB_SEC_NUM_SRC",       # irrelevant - BSID (Bloomberg Security ID Number with Source) - e.g. 1095270768082
    #  "ID_BB_UNIQUE",            # irrelevant - unique id - e.g. IX26248014-0
    #  "SECURITY_TYP",            # security type - corresponds to the Bloomberg Yellow Key - the market sector and the security type pair corresponds to the pairs listed at <SECURITY_TYPES_URL>
    #  "MARKET_SECTOR_DES",       # market sector (e.g. Equity/Bond/etc.) - the market sector and the security type pair corresponds to the pairs listed at <SECURITY_TYPES_URL>
    #  "ID_BB_GLOBAL",            # FIGI (formerly BBGID) - bloomberg global id (unique per security per exchange) - e.g. BBG009T64180
    #  "COMPOSITE_ID_BB_GLOBAL",  # bloomberg global composite id (unique per security - shared across exchanges) - **not always defined** - e.g. BBG000GGBTC7
    #  "FEED_EID1",
    #  "FEED_EID2",
    #  "FEED_EID3",
    #  "FEED_EID4",
    #  "FEED_DELAYED_EID1",
    #  "Subscription String 1",
    #  "Subscription String 2",
    #  "Subscription String 3"]
    # See http://bsym.bloomberg.com/sym/pages/bsym-whitepaper.pdf for explanation of each field
    # Example security:
    # <struct Bsym::Security
    #   name="01 COMMUNIQUE LABORATORY INC",
    #   ticker="OCQLF",
    #   pricing_source="US",
    #   bsid="399432597305",
    #   unique_id="EQ0000000000088168",
    #   security_type="Common Stock",
    #   market_sector="Equity",
    #   figi="BBG000GGBTC7",
    #   composite_bbgid="BBG000GGBTC7">
    def convert_row_to_security(row)
      name, ticker, pricing_source, bsid, unique_id, security_type, market_sector, figi, composite_bbgid = *row[0..8]

      Security.new(name, ticker, pricing_source, bsid, unique_id, security_type, market_sector, figi, composite_bbgid)
    end

    private

    def build_row(spreadsheet, row_index)
      spreadsheet.first_column.upto(spreadsheet.last_column).map {|col_index| spreadsheet.cell(row_index, col_index) }
    end

    def spreadsheet_rows(spreadsheet)
      if block_given?
        spreadsheet.first_row.upto(spreadsheet.last_row) do |row_index|
          yield build_row(spreadsheet, row_index)
        end
      else
        enum_for(:spreadsheet_rows, spreadsheet)
      end
    end

  end

end
