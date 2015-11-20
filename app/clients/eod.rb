require 'csv'
require 'zip'

module Eod
  EodBar = Struct.new()

  class Client
    ZIP_FILE_PATH = "./eod_database.zip"
    CSV_HEADER_ROW = ""


    def eod_bars
      csv_contents = get_database_as_csv
      build_eod_bars(csv_contents)
    end

    private

    def get_database_as_csv
      Quandl::Database.get('EOD').bulk_download_to_file(ZIP_FILE_PATH)

      # extract the contents from the zip file
      csv_files = begin
        Zip::File.open(ZIP_FILE_PATH) do |zip_file|
          zip_file.map do |entry|
            entry.get_input_stream.read
          end
        end
      ensure
        # delete the zip file from disk
        File.delete(ZIP_FILE_PATH)
      end

      raise "Unexpected CSV file count, #{csv_files.count}." if csv_files.count != 1

      csv_file_contents = csv_files.first

      parse_csv_file(csv_file_contents)
    end

    def build_eod_bars(csv_contents)
      rows = CSV.parse(csv_contents, headers: false, return_headers: false, skip_lines: /^(\s*,\s*)*$/)
      if rows.first.join(",") == CSV_HEADER_ROW
        rows.drop(1).map {|row| EodBar.new(*row) }
      else
        raise "The EOD bulk download CSV file doesn't conform to the expected row structure of: #{CSV_HEADER_ROW}."
      end
    end

  end
end
