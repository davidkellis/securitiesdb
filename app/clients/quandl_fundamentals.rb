require 'csv'
require 'zip'

# QuandlFundamentals retrieves the fundamentals data on US-traded securities
# The Quandl Fundamentals dataset is conceptually a set of security-attribute-value-date tuples
module QuandlFundamentals
  AttributeValue = Struct.new(:date, :value)

  class Client
    ZIP_FILE_PATH = "./data/fundamentals_database_<DATE>.zip"
    CSV_FILE_PATH = "./data/fundamentals_database_<DATE>.csv"
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

    # If called without a block:
    # all_fundamentals
    # => #<Enumerator: all_fundamentals>
    #
    # If called with a block (e.g. all_fundamentals {|ticker,indicator,dimension,attribute_values| ... }),
    # all_fundamentals invokes the block with 4 block arguments:
    # 1. ticker
    # 2. indicator
    # 3. dimension
    # 4. attribute values
    def all_fundamentals(&blk)
      if block_given?
        download_full_database
        extract_csv_file_from_zipped_database
        # delete_zipped_database
        enumerate_rowsets_in_csv(&blk)
        delete_extracted_csv_database
        nil
      else
        enum_for(:all_fundamentals)
      end
    end

    # returns a Hash of the form:
    # { symbol1 => {attributeName1 => AttributeDataset1, attributeName2 => AttributeDataset2, ...}, symbol2 => {...}, ... }
    def fundamentals(symbols = [])
      symbols.reduce({}) do |memo, symbol|
        dataset_name = "#{DATABASE_NAME}/#{symbol}"
        dataset = get_dataset(dataset_name)
        memo[symbol] = build_eod_bars_from_dataset(dataset)
        memo
      end
    end

    def download_full_database
      Quandl::Database.get(DATABASE_NAME).bulk_download_to_file(zip_file_path) unless File.exists?(zip_file_path)
    end

    private

    def csv_file_path
      @csv_file_path ||= CSV_FILE_PATH.gsub("<DATE>", Time.now.strftime("%Y%m%d"))
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
      attribute_values = []
      File.foreach(csv_file_path) do |line|
        fields = line.split(',')
        raise "CSV file malformed" unless fields.count == CSV_FIELD_COUNT
        ticker_indicator_dimension = fields[0]
        attribute_value = ::QuandlFundamentals::AttributeValue.new(
          fields[1].gsub("-","").to_i,
          fields[2].to_f
        )
        if ticker_indicator_dimension != last_ticker_indicator_dimension && last_ticker_indicator_dimension
          symbol, indicator, dimension = last_ticker_indicator_dimension.split('_')
          blk.call(symbol, indicator, dimension, attribute_values)

          attribute_values = []
        end
        attribute_values << attribute_value
        last_ticker_indicator_dimension = ticker_indicator_dimension
      end

      if last_ticker_indicator_dimension
        symbol, indicator, dimension = last_ticker_indicator_dimension.split('_')
        blk.call(symbol, indicator, dimension, attribute_values)
      end
    end

    def delete_extracted_csv_database
      File.delete(csv_file_path)
    end

    # dataset_name is a name like 'EOD/AAPL'
    def get_dataset(dataset_name)
      Quandl::Dataset.get(dataset_name)
    end

    def build_eod_bars_from_dataset(dataset)
      dataset.data.map do |record|
        ::QuandlEod::EodBar.new(
          record.date.strftime("%Y%m%d").to_i,
          record.open,
          record.high,
          record.low,
          record.close,
          record.volume,
          record.dividend,
          record.split,
          record.adj_open,
          record.adj_high,
          record.adj_low,
          record.adj_close,
          record.adj_volume
        )
      end
    end

  end
end
