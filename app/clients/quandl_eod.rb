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

  Security = Struct.new(
    :ticker,
    :name,
    :exchange,
    :last_trade_date
  )


  class Client
    ZIP_FILE_PATH = "./data/quandl_eod_database_<DATE>.zip"
    TICKER_LIST_URL = "http://static.quandl.com/end_of_day_us_stocks/ticker_list.csv"   # referenced at https://www.quandl.com/data/EOD/documentation/documentation
    TICKER_LIST_HEADER = "Ticker,Name,Exchange,Last Trade Date"
    CSV_FIELD_COUNT = 14
    DATABASE_NAME = "EOD"

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
        download_full_database
        extract_csv_file_from_zipped_database unless File.exists?(csv_file_path)
        # delete_zipped_database
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

    def download_full_database
      Quandl::Database.get(DATABASE_NAME).bulk_download_to_file(zip_file_path) unless File.exists?(zip_file_path)
    end

    private

    # todo: this needs to change so that the date
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

    # csv_file_path is a CSV file of the form:
    # A,1999-11-18,45.5,50.0,40.0,44.0,44739900.0,0.0,1.0,29.84158347724813,32.792948876096844,26.234359100877477,28.857795010965223,44739900.0
    # A,1999-11-19,42.94,43.0,39.81,40.38,10897100.0,0.0,1.0,28.161733875159676,28.20108422524141,26.108957279229315,26.482785605005773,10897100.0
    # ...
    # ZZZ,2015-07-16,0.5,0.5,0.5,0.5,0.0,0.0,1.0,0.5,0.5,0.5,0.5,0.0
    # ZZZ,2015-07-17,1.0,1.01,1.0,1.0,1000.0,0.0,1.0,1.0,1.01,1.0,1.0,1000.0
    def enumerate_rows_in_csv(&blk)
      File.foreach(csv_file_path) do |line|
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

    # csv_file_path is a CSV file of the form:
    # A,1999-11-18,45.5,50.0,40.0,44.0,44739900.0,0.0,1.0,29.84158347724813,32.792948876096844,26.234359100877477,28.857795010965223,44739900.0
    # A,1999-11-19,42.94,43.0,39.81,40.38,10897100.0,0.0,1.0,28.161733875159676,28.20108422524141,26.108957279229315,26.482785605005773,10897100.0
    # ...
    # ZZZ,2015-07-16,0.5,0.5,0.5,0.5,0.0,0.0,1.0,0.5,0.5,0.5,0.5,0.0
    # ZZZ,2015-07-17,1.0,1.01,1.0,1.0,1000.0,0.0,1.0,1.0,1.01,1.0,1.0,1000.0
    def enumerate_rowsets_in_csv(&blk)
      last_symbol = nil
      eod_bars = []
      File.foreach(csv_file_path) do |line|
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

    def get_securities(url)
      csv_contents = Net::HTTP.get(URI(url))
      # csv_contents.encode!("UTF-8", "ISO-8859-1")   # CSI Data encodes their CSV files with the ISO-8859-1 character set, so we need to convert it to UTF-8
      rows = CSV.parse(csv_contents, headers: false, return_headers: false, skip_lines: /^(\s*,\s*)*$/)
      if rows.first.join(",") == TICKER_LIST_HEADER
        rows.drop(1).map {|row| Security.new(*row.map{|s| s && s.strip }) }
      else
        raise "The securities list in #{url} doesn't conform to the expected row structure of: #{TICKER_LIST_HEADER}."
      end
    end

  end
end
