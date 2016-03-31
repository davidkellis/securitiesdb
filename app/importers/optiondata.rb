# this imports historical options prices from the zip files downloaded from http://optiondata.net/
class OptionDataImporter
  LegacyBasicRecord = Struct.new(
    :underlying,
    :underlying_price,
    :expiry,
    :call_or_put,   # type
    :strike,
    :last,
    :bid,
    :ask,
    :volume,
    :open_interest
  )

  BasicRecord = Struct.new(
    :underlying,
    :underlying_price,
    :expiry,
    :call_or_put,   # type
    :strike,
    :last,
    :bid,
    :ask,
    :volume,
    :open_interest,
    :open,
    :high,
    :low,
    :previous_close,
    :dollar_change,
    :percent_change,
    :bid_size,
    :ask_size,
    :contract_high,
    :contract_low,
    :contract_type,
    :last_trade_time,
    :underlying_volume
  )


  def initialize(zip_file_paths)
    @zip_file_paths = zip_file_paths
  end

  def import
    extracted_csv_file_paths = extract_csv_files_from_zipped_databases(@zip_file_paths)
    extracted_csv_file_paths.each do |csv_file_path|
      #   iterate over lines in file - for each line:
      #     read line in as either a BasicRecord or a LegacyBasicRecord depending on the type of file
      #     persist BasicRecord or LegacyBasicRecord to database via the Option and EodOptionQuote models
    end
  end

  private

  def log(msg)
    Application.logger.info("#{Time.now} - #{msg}")
  end

  # returns array of file paths representing paths to extracted files
  def extract_csv_files_from_zipped_databases(zip_file_paths)
    zip_file_paths.map do |zip_file_path|
      base_directory = File.dirname(zip_file_path)
      filename_wo_ext = File.basename(zip_file_path, ".zip")
      extraction_directory = File.join(base_directory, filename_wo_ext)

      puts "Creating extraction directory: #{extraction_directory}"
      FileUtils.mkdir_p(extraction_directory)

      puts "Processing #{zip_file_path}:"
      extracted_paths = []
      Zip::File.open(zip_file_path) do |zip_file|
        zip_file.each do |entry|
          # Extract each file
          destination_path = File.join(extraction_directory, entry.name)
          log "Extracting #{entry.name} -> #{destination_path}"
          if !File.exists?(destination_path)
            entry.extract(destination_path)
            extracted_paths << destination_path
          end
        end
      end
      extracted_paths
    end.flatten.uniq
  end

end
