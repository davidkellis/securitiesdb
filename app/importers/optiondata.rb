# this imports historical options prices from the zip files downloaded from http://optiondata.net/
class OptionDataImporter
  # example row:
  # Underlying,UnderlyingPrice,Expiry,Type,Strike,Last,Bid,Ask,Volume,OpenInterest
  # A,33.50,20060121,C,10.0,24.4,23.4,23.6,0,257
  LegacyBasicRecord = Struct.new(
    :observation_date,    # an integer datestamp yyyymmdd
    :underlying,
    :underlying_price,
    :expiry,
    :call_or_put,         # type
    :strike,
    :last,
    :bid,
    :ask,
    :volume,
    :open_interest
  )

  # example row:
  # Underlying,Underlying Price,Expiry,Type,Strike,Open,Last,High,Low,Bid,Ask,Bid Size,Ask Size,Volume,Open Interest,Change,Prev Close,Change Percent,Contract High,Contract Low,Contract Type,Last Trade Date,Underlying Volume
  # A,41.81,20160115,C,25,0,11.3,0,0,14.25,15.9,54,170,0,50,0,11.3,0,18,11.3,STAN,2015-10-12T09:33:07-04:00,1450968
  BasicRecord = Struct.new(
    :observation_date,    # an integer datestamp yyyymmdd
    :underlying,
    :underlying_price,
    :expiry,
    :call_or_put,         # type
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


  LEGACY_BASIC_HEADER     = "Underlying,UnderlyingPrice,Expiry,Type,Strike,Last,Bid,Ask,Volume,OpenInterest"
  LEGACY_BASIC_ROW_FIELD_COUNT  = LEGACY_BASIC_HEADER.split(',').count
  BASIC_HEADER            = "Underlying,Underlying Price,Expiry,Type,Strike,Last,Bid,Ask,Volume,OpenInterest,Open,High,Low,Prev Close,Change,Change Percent,Bid Size,Ask Size,Contract High,Contract Low,Contract Type,Last Trade Date,Underlying Volume"
  BASIC_ROW_FIELD_COUNT   = BASIC_HEADER.split(',').count


  def initialize(zip_file_paths)
    @zip_file_paths = zip_file_paths
  end

  def import
    extracted_csv_file_paths = extract_csv_files_from_zipped_databases(@zip_file_paths)
    extracted_csv_file_paths.each do |csv_file_path|
      enumerate_records_in_csv(csv_file_path) do |record|
        puts record.inspect
        return
        #     persist BasicRecord or LegacyBasicRecord to database via the Option and EodOptionQuote models
      end
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
          end
          extracted_paths << destination_path
        end
      end
      extracted_paths
    end.flatten.uniq
  end

  def enumerate_records_in_csv(csv_file_path, &blk)
    observation_datestamp = /options_([0-9]{8})\.csv/.match(csv_file_path)[1].to_i

    first_line = File.open(csv_file_path, &:readline).strip

    row_extractor_fn = case first_line
    when LEGACY_BASIC_HEADER
      ->(line) do
        fields = line.split(',')
        raise "CSV file malformed: #{line} should have #{LEGACY_BASIC_ROW_FIELD_COUNT} fields" unless fields.count == LEGACY_BASIC_ROW_FIELD_COUNT

        # Underlying,UnderlyingPrice,Expiry,Type,Strike,Last,Bid,Ask,Volume,OpenInterest
        # A,33.50,20060121,C,10.0,24.4,23.4,23.6,0,257
        ::OptionDataImporter::LegacyBasicRecord.new(
          observation_datestamp,    # :observation_date,    # an integer datestamp yyyymmdd
          fields[0], 					      # :underlying,
          fields[1].to_f, 			    # :underlying_price,
          fields[2].to_i,			      # :expiry,
          fields[3], 					      # :call_or_put,         # type
          fields[4].to_f, 					# :strike,
          fields[5].to_f, 					# :last,
          fields[6].to_f, 					# :bid,
          fields[7].to_f, 					# :ask,
          fields[8].to_i, 					# :volume,
          fields[9].to_i 					  # :open_interest
        )
      end
    when BASIC_HEADER
      ->(line) do
        fields = line.split(',')
        raise "CSV file malformed: #{line} should have #{BASIC_ROW_FIELD_COUNT} fields" unless fields.count == BASIC_ROW_FIELD_COUNT

        # Underlying,Underlying Price,Expiry,Type,Strike,Open,Last,High,Low,Bid,Ask,Bid Size,Ask Size,Volume,Open Interest,Change,Prev Close,Change Percent,Contract High,Contract Low,Contract Type,Last Trade Date,Underlying Volume
        # A,41.81,20160115,C,25,0,11.3,0,0,14.25,15.9,54,170,0,50,0,11.3,0,18,11.3,STAN,2015-10-12T09:33:07-04:00,1450968
        ::OptionDataImporter::BasicRecord.new(
          observation_datestamp,    # :observation_date,    # an integer datestamp yyyymmdd
          fields[0], 					      # :underlying,
          fields[1].to_f, 			    # :underlying_price,
          fields[2].to_i,			      # :expiry,
          fields[3], 					      # :call_or_put,         # type
          fields[4].to_f, 					# :strike,
          fields[5].to_f, 					# :last,
          fields[6].to_f, 					# :bid,
          fields[7].to_f, 					# :ask,
          fields[8].to_i, 					# :volume,
          fields[9].to_i, 					# :open_interest
          fields[10].to_f,           # :open,
          fields[11].to_f,           # :high,
          fields[12].to_f,           # :low,
          fields[13].to_f,           # :previous_close,
          fields[14].to_f,           # :dollar_change,
          fields[15].to_f,           # :percent_change,
          fields[16].to_i,           # :bid_size,
          fields[17].to_i,           # :ask_size,
          fields[18].to_f,           # :contract_high,
          fields[19].to_f,           # :contract_low,
          fields[20],                # :contract_type,
          fields[21],                # :last_trade_time,     # todo: parse this into a Time object
          fields[22].to_i            # :underlying_volume
        )
      end
    else
      raise "CSV file #{csv_file_path} has an unknown file structure. The first line doesn't match either expected header row structure."
    end

    line_count = 1
    File.foreach(csv_file_path) do |line|
      if line_count > 1
        record = row_extractor_fn.call(line)
        blk.call(record)
      end
      line_count += 1
    end
  end

end