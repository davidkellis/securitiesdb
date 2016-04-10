require 'csv'
require 'net/http'
require 'zip'

# QuandlFundamentals retrieves the fundamentals data on US-traded securities
# The Quandl Fundamentals dataset is conceptually a set of security-attribute-value-date tuples
module QuandlFundamentals
  Indicator = Struct.new(:label, :title, :available_dimensions, :statement, :description, :na_value, :units)
  IndicatorValue = Struct.new(:date, :value)

  Security = Struct.new(
    :ticker,
    :name,
    :cusip,
    :isin,
    :currency,
    :sector,
    :industry,
    :last_updated,
    :prior_tickers,
    :ticker_change_date,
    :related_tickers,
    :exchange,
    :sic,
    :perma_ticker,
    :location,
    :delisted_from,
    :is_foreign
  )

  class Client
    ZIP_FILE_PATH = "./data/quandl_fundamentals_database_<DATE>.zip"
    TICKER_LIST_URL = "http://www.sharadar.com/meta/tickers.txt"        # referenced at https://www.quandl.com/data/SF1/documentation/tickers
    TICKER_LIST_HEADER = "Ticker	Name	CUSIP	ISIN	Currency	Sector	Industry	Last Updated	Prior Tickers	Ticker Change Date	Related Tickers	Exchange	SIC	Perma Ticker	Location	Delisted From	Is Foreign"
    INDICATORS_URL = "http://www.sharadar.com/meta/indicators.txt"      # referenced at https://www.quandl.com/data/SF1/documentation/indicators
    INDICATOR_LISTING_HEADER = ["Indicator", "Title", "Available Dimensions", "Statement", "Description", "NA Value", "Units"]
    CSV_FIELD_COUNT = 3
    DATABASE_NAME = "SF1"

    attr_accessor :logger, :zip_file_path

    def initialize(logger, target_zip_file_path = nil)
      @zip_file_path = target_zip_file_path || ZIP_FILE_PATH.gsub("<DATE>", Time.now.strftime("%Y%m%d"))
      @logger = logger
    end

    def log(msg)
      Application.logger.info("#{Time.now} - #{msg}")
    end

    def securities
      get_securities(TICKER_LIST_URL)
    end

    def indicators
      tsv_contents = Net::HTTP.get(URI(INDICATORS_URL))
      tsv_contents.encode!("UTF-8", "ISO-8859-1")   # Sharadar encodes their CSV files with the ISO-8859-1 character set, so we need to convert it to UTF-8
      rows = CSV.parse(tsv_contents, headers: false, return_headers: false, col_sep: "\t")

      if rows.first == INDICATOR_LISTING_HEADER
        rows.drop(1).map {|row| Indicator.new(*row.map{|s| s && s.strip }) }
      else
        raise "The securities list in #{INDICATORS_URL} doesn't conform to the expected row structure of: #{INDICATOR_LISTING_HEADER}."
      end
    end

    # If called without a block:
    # all_fundamentals
    # => #<Enumerator: all_fundamentals>
    #
    # If called with a block (e.g. all_fundamentals {|ticker,indicator,dimension,indicator_values| ... }),
    # all_fundamentals invokes the block with 4 block arguments:
    # 1. ticker
    # 2. indicator
    # 3. dimension
    # 4. attribute values
    def all_fundamentals(&blk)
      if block_given?
        download_full_database
        extract_csv_file_from_zipped_database unless File.exists?(csv_file_path)
        # delete_zipped_database
        enumerate_rowsets_in_csv(&blk)
        delete_extracted_csv_database
        nil
      else
        enum_for(:all_fundamentals)
      end
    end

    def download_full_database
      Quandl::Database.get(DATABASE_NAME).bulk_download_to_file(zip_file_path) unless File.exists?(zip_file_path)
    end

    private

    def csv_file_path
      @csv_file_path ||= zip_file_path.gsub(/\.zip$/, ".csv")
    end

    def extract_csv_file_from_zipped_database
      Zip::File.open(zip_file_path) do |zip_file|
        # Handle entries one by one; NOTE: there should only be a single file in the zipfile
        zip_file.each do |entry|
          # Extract file
          log "Extracting #{entry.name} to #{csv_file_path}"
          entry.extract(csv_file_path)
        end
      end
    end

    def delete_zipped_database
      File.delete(zip_file_path)
    end

    # If called with a block (e.g. all_fundamentals {|ticker,indicator,dimension,indicator_values| ... }),
    # all_fundamentals invokes the block with 4 block arguments:
    # 1. ticker
    # 2. indicator
    # 3. dimension
    # 4. attribute values
    #
    # csv_file_path is a CSV file of the form:
    # AAPL_ACCOCI_ARQ,2004-02-10,-31000000.0
    # AAPL_ACCOCI_ARQ,2004-05-06,-10000000.0
    # AAPL_ACCOCI_ARQ,2004-08-05,-18000000.0
    # ...
    # AAPL_ACCOCI_MRQ,2004-03-27,-10000000.0
    # AAPL_ACCOCI_MRQ,2004-06-26,-18000000.0
    # AAPL_ACCOCI_MRQ,2004-09-25,-15000000.0
    # ...
    def enumerate_rowsets_in_csv(&blk)
      last_ticker_indicator_dimension = nil
      indicator_values = []
      File.foreach(csv_file_path) do |line|
        fields = line.split(',')
        raise "CSV file malformed" unless fields.count == CSV_FIELD_COUNT
        ticker_indicator_dimension = fields[0]
        indicator_value = if ticker_indicator_dimension.end_with?("_EVENT")
          sign, significant_digits, base, exponent = BigDecimal.new(fields[2]).split
          ::QuandlFundamentals::IndicatorValue.new(
            fields[1].gsub("-","").to_i,
            significant_digits.to_f    # this is to remove meaningless zeros at the tail end of the EVENT value (e.g. AAL_EVENT,2013-12-09,1.1122123333232e+25); see http://www.sharadar.com/meta/indicator/EVENT
          )
        else
          ::QuandlFundamentals::IndicatorValue.new(
            fields[1].gsub("-","").to_i,
            fields[2].to_f
          )
        end
        if ticker_indicator_dimension != last_ticker_indicator_dimension && last_ticker_indicator_dimension
          ticker, indicator, dimension = last_ticker_indicator_dimension.split('_')
          blk.call(ticker, indicator, dimension, indicator_values)

          indicator_values = []
        end
        indicator_values << indicator_value
        last_ticker_indicator_dimension = ticker_indicator_dimension
      end

      if last_ticker_indicator_dimension
        ticker, indicator, dimension = last_ticker_indicator_dimension.split('_')
        blk.call(ticker, indicator, dimension, indicator_values)
      end
    end

    def delete_extracted_csv_database
      File.delete(csv_file_path)
    end

    def get_securities(url)
      csv_contents = Net::HTTP.get(URI(url))
      # csv_contents.encode!("UTF-8", "ISO-8859-1")   # CSI Data encodes their CSV files with the ISO-8859-1 character set, so we need to convert it to UTF-8
      rows = CSV.parse(csv_contents, col_sep: "\t", headers: false, return_headers: false, skip_lines: /^(\s*,\s*)*$/)
      if rows.first.join("\t") == TICKER_LIST_HEADER
        rows.drop(1).map {|row| ::QuandlFundamentals::Security.new(*row.map{|s| s && s.strip }) }
      else
        raise "The securities list in #{url} doesn't conform to the expected row structure of: #{TICKER_LIST_HEADER}."
      end
    end

  end
end
