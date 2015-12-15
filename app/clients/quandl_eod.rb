require 'csv'
require 'zip'

module QuandlEod
  # Per https://www.quandl.com/data/EOD/documentation/documentation:
  # The dividend column reflects the dollar amount of any cash dividend with ex-date on that day. If there is no dividend, this column has value 0.
  # The split_adjustment_factor reflects the ratio of the number of new shares to the number of old shares, assuming a split with ex-date on that day. If there is no split, this column has value 1.
  EodBar = Struct.new(
    :date,
    :unadjusted_open,
    :unadjusted_high,
    :unadjusted_low,
    :unadjusted_close,
    :unadjusted_volume,
    :dividend,
    :split_adjustment_factor,
    :adjusted_open,
    :adjusted_high,
    :adjusted_low,
    :adjusted_close,
    :adjusted_volume
  )

  class Client
    ZIP_FILE_PATH = "./eod_database.zip"
    CSV_FILE_PATH = "./eod_database.csv"
    CSV_FIELD_COUNT = 14
    DATABASE_NAME = "EOD"

    attr_accessor :logger

    def initialize(logger)
      @logger = logger
    end

    def log(msg)
      Application.logger.info("#{Time.now} - #{msg}")
    end

    # If called without a block:
    # all_eod_bars
    # => #<Enumerator: all_eod_bars>
    #
    # If called with a block:
    # all_eod_bars {|symbol, eod_bars| puts "#{symbol} - #{eod_bars.inspect}" }
    # AAPL - [EodBar1, EodBar2, ...]
    # ABC - [EodBar1000, EodBar1001, ...]
    # ...
    # => nil
    def all_eod_bars(&blk)
      if block_given?
        download_zipped_database
        extract_csv_file_from_zipped_database
        delete_zipped_database
        enumerate_rowsets_in_csv(&blk)
        delete_extracted_csv_database
        nil
      else
        enum_for(:all_eod_bars)
      end
    end

    # returns hash of the form:
    # { "AAPL" => [EodBar1, EodBar2, ...],
    #   "MSFT" => [EodBar1000, ...],
    #   ... }
    def eod_bars(symbols = [])
      symbols.reduce({}) do |memo, symbol|
        dataset_name = "#{DATABASE_NAME}/#{symbol}"
        dataset = get_dataset(dataset_name)
        memo[symbol] = build_eod_bars_from_dataset(dataset)
        memo
      end
    end

    private

    def download_zipped_database
      Quandl::Database.get(DATABASE_NAME).bulk_download_to_file(ZIP_FILE_PATH)
    end

    def extract_csv_file_from_zipped_database
      Zip::File.open(ZIP_FILE_PATH) do |zip_file|
        # Handle entries one by one; NOTE: there should only be a single file in the zipfile
        zip_file.each do |entry|
          # Extract file
          log "Extracting #{entry.name} to #{CSV_FILE_PATH}"
          entry.extract(CSV_FILE_PATH)
        end
      end
    end

    def delete_zipped_database
      File.delete(ZIP_FILE_PATH)
    end

    # CSV_FILE_PATH is a CSV file of the form:
    # A,1999-11-18,45.5,50.0,40.0,44.0,44739900.0,0.0,1.0,29.84158347724813,32.792948876096844,26.234359100877477,28.857795010965223,44739900.0
    # A,1999-11-19,42.94,43.0,39.81,40.38,10897100.0,0.0,1.0,28.161733875159676,28.20108422524141,26.108957279229315,26.482785605005773,10897100.0
    # ...
    # ZZZ,2015-07-16,0.5,0.5,0.5,0.5,0.0,0.0,1.0,0.5,0.5,0.5,0.5,0.0
    # ZZZ,2015-07-17,1.0,1.01,1.0,1.0,1000.0,0.0,1.0,1.0,1.01,1.0,1.0,1000.0
    def enumerate_rows_in_csv(&blk)
      File.foreach(CSV_FILE_PATH) do |line|
        fields = line.split(',')
        raise "CSV file malformed" unless fields.count == CSV_FIELD_COUNT
        symbol = fields[0]
        eod_bar = ::QuandlEod::EodBar.new(
          fields[1].gsub("-","").to_i,
          fields[2].to_f,
          fields[3].to_f,
          fields[4].to_f,
          fields[5].to_f,
          fields[6].to_f,
          fields[7].to_f,
          fields[8].to_f,
          fields[9].to_f,
          fields[10].to_f,
          fields[11].to_f,
          fields[12].to_f,
          fields[13].to_f
        )
        blk.call(symbol, eod_bar)
      end
    end

    # CSV_FILE_PATH is a CSV file of the form:
    # A,1999-11-18,45.5,50.0,40.0,44.0,44739900.0,0.0,1.0,29.84158347724813,32.792948876096844,26.234359100877477,28.857795010965223,44739900.0
    # A,1999-11-19,42.94,43.0,39.81,40.38,10897100.0,0.0,1.0,28.161733875159676,28.20108422524141,26.108957279229315,26.482785605005773,10897100.0
    # ...
    # ZZZ,2015-07-16,0.5,0.5,0.5,0.5,0.0,0.0,1.0,0.5,0.5,0.5,0.5,0.0
    # ZZZ,2015-07-17,1.0,1.01,1.0,1.0,1000.0,0.0,1.0,1.0,1.01,1.0,1.0,1000.0
    def enumerate_rowsets_in_csv(&blk)
      last_symbol = nil
      eod_bars = []
      File.foreach(CSV_FILE_PATH) do |line|
        fields = line.split(',')
        raise "CSV file malformed" unless fields.count == CSV_FIELD_COUNT
        symbol = fields[0]
        eod_bar = ::QuandlEod::EodBar.new(
          fields[1].gsub("-","").to_i,
          fields[2].to_f,
          fields[3].to_f,
          fields[4].to_f,
          fields[5].to_f,
          fields[6].to_f,
          fields[7].to_f,
          fields[8].to_f,
          fields[9].to_f,
          fields[10].to_f,
          fields[11].to_f,
          fields[12].to_f,
          fields[13].to_f
        )
        if symbol != last_symbol && last_symbol
          blk.call(last_symbol, eod_bars)

          eod_bars = []
        end
        eod_bars << eod_bar
        last_symbol = symbol
      end

      blk.call(last_symbol, eod_bars) if last_symbol
    end

    def delete_extracted_csv_database
      File.delete(CSV_FILE_PATH)
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
