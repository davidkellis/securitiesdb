require 'lru_redux'

# this imports historical options prices from the zip files downloaded from http://optiondata.net/
class OptionDataImporter
  # example row:
  # Underlying,UnderlyingPrice,Expiry,Type,Strike,Last,Bid,Ask,Volume,OpenInterest
  # A,33.50,20060121,C,10.0,24.4,23.4,23.6,0,257
  LegacyBasicRecord = Struct.new(
    :observation_date,    # an integer datestamp yyyymmdd
    :underlying,
    :underlying_price,
    :expiry,              # an integer datestamp yyyymmdd
    :call_or_put,         # type - C or P
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
    :expiry,              # an integer datestamp yyyymmdd
    :call_or_put,         # type - C or P
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

  AMERICAN_STYLE_OPTION   = 'A'
  EQUITY_OPTION = 'Equity Option'


  def initialize(zip_file_paths)
    @zip_file_paths = zip_file_paths
    @underlying_security_cache = LruRedux::Cache.new(20)
  end

  def import
    extracted_csv_file_paths = extract_csv_files_from_zipped_databases(@zip_file_paths)
    extracted_csv_file_paths.each do |csv_file_path|
      puts "Importing #{csv_file_path}"
      enumerate_records_in_csv(csv_file_path) do |record|
        import_option(record)
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

  # todo: refactor this
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
          fields[3].upcase, 				# :call_or_put,         # type = C or P
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
          fields[3].upcase, 				# :call_or_put,         # type = C or P
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

  # this script assumes all options are exclusively listed on the CBOE; all newly created option Securities are listed *only* on the CBOE
  def import_option(record)
    # per http://optiondata.net/collections/yearly-historical-options-data-sets:
    # "The Basic Data Package includes every optionable equity symbol available for its specific year.
    # We gather data from every US based option exchange including NASDAQ, NYSE, AMEX, CBOE, BATS, BOX, C2, MIAX, PHLX, and ARCA."
    underlying_security = find_underlying_security(record)

    if underlying_security
      # look up option for given underlying security, expiration, type (call/put), strike, and style (american/european)
      # todo: remove the hardcoded 'A' from the query - we need to determine whether given option (i.e. <record>) is an American or European style option
      option = find_option(underlying_security, record.expiry, record.call_or_put, record.strike, AMERICAN_STYLE_OPTION) || create_option(underlying_security, record, AMERICAN_STYLE_OPTION)

      if option
        update_listed_security_start_date_if_needed(option, record)

        find_eod_option_quote(option.id, record.observation_date) || create_eod_option_quote(option.id, record)
      else
        log "Unable to find or create option referenced by: record=#{record.to_h}"
      end
    end
  end

  def find_underlying_security(record)
    @underlying_security_cache.getset("#{record.underlying},#{record.observation_date}") do
      # per http://optiondata.net/collections/yearly-historical-options-data-sets:
      # "The Basic Data Package includes every optionable equity symbol available for its specific year.
      # We gather data from every US based option exchange including NASDAQ, NYSE, AMEX, CBOE, BATS, BOX, C2, MIAX, PHLX, and ARCA."
      underlying_securities = FindSecurity.us_exchanges.all(record.underlying, record.observation_date)
      underlying_security = case underlying_securities.count
      when 0
        log "Unable to import option. Can't find underlying security: underlying=#{record.underlying} observation_date=#{record.observation_date}"
        nil
      when 1
        underlying_securities.first
      else
        log "Unable to import option. Underlying security symbol ambiguously identifies #{underlying_securities.count} securities:\nrecord=#{record.to_h}\nmatched underlying securities=#{underlying_securities.map(&:to_hash).join("\n")}"
        nil
      end
    end
  end

  # call_or_put is designated with 'C' or 'P'
  # american_or_european is designated with 'A' or 'E'
  def find_option(underlying_security, expiration, call_or_put, strike, american_or_european)
    underlying_security.options_dataset.where(expiration: expiration, type: call_or_put, strike: strike, style: american_or_european).first
  end

  def create_option(underlying_security, record, american_or_european)
    option_symbol = occ_option_symbol_from_record(record)
    security = CreateSecurity.run(option_symbol, EQUITY_OPTION)
    option = Option.create(
      security_id: security.id,
      underlying_security_id: underlying_security.id,
      expiration: record.expiry,
      type: record.call_or_put,
      strike: record.strike,
      style: american_or_european
    )
    listed_security = ListedSecurity.create(
      exchange_id: cboe.id,                           # create a single ListedSecurity for the option, and list it on the CBOE
      security_id: security.id,
      symbol: option_symbol,
      listing_start_date: record.observation_date,    # we start with the observation date of the current record, and then update it later if we observe any earlier record of this option's price
      listing_end_date: record.expiry
    )
    option
  end

  def update_listed_security_start_date_if_needed(option, record)
    listed_securities = option.security.listed_securities_dataset.where(exchange_id: cboe.id).all

    if listed_securities.count == 1
      listed_security = listed_securities.first
      listed_security.update(listing_start_date: record.observation_date) if record.observation_date < listed_security.listing_start_date
    else
      log("Error: Expected one CBOE listing for option #{option.to_hash}, but found the following listings: #{listed_securities.inspect}")
    end
  end

  def find_eod_option_quote(option_id, observation_date)
    EodOptionQuote.first(option_id: option_id, date: observation_date)
  end

  def create_eod_option_quote(option_id, record)
    EodOptionQuote.create(
      option_id: option_id,
      date: record.observation_date,
      last: record.last,
      bid: record.bid,
      ask: record.ask,
      volume: record.volume,
      open_interest: record.open_interest
    )
  end

  def cboe
    @cboe ||= Exchange.cboe.first
  end

  # https://en.wikipedia.org/wiki/Option_symbol#The_OCC_Option_Symbol
  # The OCC option symbol consists of 4 parts:
  #
  # Root symbol of the underlying stock or ETF, padded with spaces to 6 characters
  # Expiration date, 6 digits in the format yymmdd
  # Option type, either P or C, for put or call
  # Strike price, as the price x 1000, front padded with 0s to 8 digits
  #
  # Examples:
  #
  # SPX   141122P00019500
  # This symbol represents a put on SPX, expiring on 11/22/2014, with a strike price of $19.50.
  #
  # LAMR  150117C00052500
  # This symbol represents a call on LAMR, expiring on 1/17/2015, with a strike price of $52.50.
  def occ_option_symbol(underlying_symbol, expiration_yymmdd, call_or_put, strike_price)
    root_symbol = underlying_symbol.strip.upcase.ljust(6, ' ')
    expiration = expiration_yymmdd
    type = call_or_put
    strike = (strike_price.to_f * 1000).round.to_s.rjust(8, '0')
    "#{root_symbol}#{expiration}#{type}#{strike}"
  end

  def occ_option_symbol_from_record(record)
    occ_option_symbol(
      record.underlying,
      record.expiry.to_s[2...8],    # extract the yymmdd from the yyyymmdd-formatted datestamp
      record.call_or_put,
      record.strike
    )
  end

end
