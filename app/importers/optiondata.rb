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
    # extract zip files into directory
    # iterate over option_<date>.csv files in directory - for each csv file:
    #   iterate over lines in file - for each line:
    #     read line in as either a BasicRecord or a LegacyBasicRecord depending on the type of file
    #     persist BasicRecord or LegacyBasicRecord to database via the Option and EodOptionQuote models
  end
end
