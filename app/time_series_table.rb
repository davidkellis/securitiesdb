require 'java'

class TimeSeriesTable
  # date_value_map is a java.util.NavigableMap (java.util.TreeMap) of datestamp/value pairs
  # missing_values_behavior must be one of the following symbols:
  #   :omit - do not fill in missing values
  #   :most_recent - fill in missing values using the most recent prior observation
  #   :most_recent_or_omit - fill in missing values using the most recent prior observation; if most recent value doesn't exist, omit
  Column = Struct.new(:name, :date_value_map, :missing_values_behavior)

  attr_accessor :columns

  def initialize()
    @columns = {}
  end

  # date_value_pairs is an Enumerable of datestamp/value pairs, each of the form: [datestamp, value]
  # missing_values_behavior must be one of the following symbols:
  #   :omit - do not fill in missing values
  #   :most_recent - fill in missing values using the most recent prior observation (if most recent value doesn't exist, use nil as placeholder)
  #   :most_recent_or_omit - fill in missing values using the most recent prior observation; if most recent value doesn't exist, omit
  def add_column(column_name, datestamp_value_pairs, missing_values_behavior = :omit)
    @columns[column_name] = Column.new(column_name, build_navigable_map(datestamp_value_pairs), missing_values_behavior)
  end

  def to_a(include_column_headers = true)
    common_dates = identify_common_dates_among_core_columns
    sorted_common_dates = common_dates.to_a.sort
    table = sorted_common_dates.map do |datestamp|
      row = columns.values.map do |column|
        case column.missing_values_behavior
        when :omit
          column.date_value_map[datestamp]    # this is assured to return a value because sorted_common_dates only contains datestamps that are common to all the "none" columns
        when :most_recent
          most_recent_value(column.date_value_map, datestamp)
        when :most_recent_or_omit
          most_recent_value(column.date_value_map, datestamp) || break
        else
          raise "Unknown missing value behavior, \"#{column.missing_values_behavior}\", for column #{column.name}."
        end
      end
      row.unshift(datestamp) if row
    end
    table.compact!    # remove any rows that were nil due to being omitted
    table.unshift(columns.values.map(&:name).unshift("Date")) if include_column_headers
    table
  end

  def to_csv
    to_a.map {|row| row.join(',') }.join("\n")
  end

  def no_fill_columns
    columns.values.select {|col| col.missing_values_behavior == :omit }
  end

  def identify_common_dates_among_core_columns
    core_columns = no_fill_columns
    if !core_columns.empty?
      first_key_set = core_columns.first.date_value_map.keySet.to_set
      core_columns.reduce(first_key_set) {|common_key_set, column| common_key_set & column.date_value_map.keySet.to_set }
    end
  end

  def most_recent_value(navigable_map, datestamp)
    key = navigable_map.floorKey(datestamp)
    navigable_map[key] if key
  end

  # datestamp_value_pairs is an array of datestamp/value pairs, each of the form: [datestamp, value]
  def build_navigable_map(datestamp_value_pairs)
    map = java.util.TreeMap.new
    datestamp_value_pairs.each {|pair| map.put(pair[0], pair[1]) }
    map
  end
end

def build_table
  apple = LookupSecurity.us_stocks.run("AAPL")
  google = LookupSecurity.us_stocks.run("GOOGL")
  microsoft = LookupSecurity.us_stocks.run("MSFT")
  exxon = LookupSecurity.us_stocks.run("XOM")
  ge = LookupSecurity.us_stocks.run("GE")
  jnj = LookupSecurity.us_stocks.run("JNJ")
  amazon = LookupSecurity.us_stocks.run("AMZN")
  wellsfargo = LookupSecurity.us_stocks.run("WFC")
  berkshire_hathaway = LookupSecurity.us_stocks.run("BRK.B")
  jpmorgan = LookupSecurity.us_stocks.run("JPM")

  xiv = LookupSecurity.us_stocks.run("XIV")
  vxx = LookupSecurity.us_stocks.run("VXX")

  vix = LookupSecurity.us_indices.run("VIX Index")
  sp500 = LookupSecurity.us_indices.run("SPX Index")

  table = TimeSeriesTable.new
  table.add_column("AAPL EOD", apple.eod_bars.map {|bar| [bar.date, bar.close.to_f] })
  table.add_column("AAPL EPS", LookupFundamentals.all(apple, "EPS", "ARQ").map {|fdp| [fdp.start_date, fdp.value.to_f] }, :most_recent_or_omit)
  table.to_csv
end
