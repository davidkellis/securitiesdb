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

  def to_a(include_column_headers = true, include_date_column = true)
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
      row.unshift(datestamp) if row && include_date_column
      row
    end
    table.compact!    # remove any rows that were nil due to being omitted
    if include_column_headers
      column_headers = columns.values.map(&:name)
      column_headers.unshift("Date") if include_date_column
      table.unshift(column_headers)
    end
    table
  end

  def to_csv(include_column_headers = true, include_date_column = true)
    to_a(include_column_headers, include_date_column).map {|row| row.join(',') }.join("\n")
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

class TimeSeriesTableBuilder
  def eod_bars_to_close_column(security)
    security.eod_bars.map {|bar| [bar.date, bar.close.to_f] }
  end

  # returns array of the form:
  # [
  #   {:attribute_name=>"Accumulated Other Comprehensive Income",
  #    :attribute_label=>"ACCOCI",
  #    :dimension_name=>"ARQ"},
  #   {:attribute_name=>"Accumulated Other Comprehensive Income",
  #    :attribute_label=>"ACCOCI",
  #    :dimension_name=>"ARY"},
  #   ...
  # ]
  def identify_fundamentals_tracked_for(security)
    Database.connection.fetch(
      """
        select distinct attr.name AS attribute_name, attr.label AS attribute_label, dim.name AS dimension_name
        from fundamental_datasets ds
        inner join fundamental_attributes attr on attr.id = ds.fundamental_attribute_id
        inner join fundamental_dimensions dim on dim.id = ds.fundamental_dimension_id
        where ds.security_id = ?
      """,
      security.id
    )
  end

  def identify_arq_fundamentals_tracked_for(security)
    rows = identify_fundamentals_tracked_for(security)
    rows.select {|row| row[:dimension_name] == FundamentalDimension::ARQ }
  end

  def identify_inst_fundamentals_tracked_for(security)
    rows = identify_fundamentals_tracked_for(security)
    rows.select {|row| row[:dimension_name] == FundamentalDimension::INSTANTANEOUS }
  end

  def fundamentals_column(security, attribute_name, dimension_name)
    LookupFundamentals.all_observations(security, attribute_name, dimension_name).
      map {|fundamental_observation| [fundamental_observation.date, fundamental_observation.value.to_f] }
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


    arq_attribute_dimension_triples = identify_arq_fundamentals_tracked_for(apple)
    inst_attribute_dimension_triples = identify_inst_fundamentals_tracked_for(apple)

    table = TimeSeriesTable.new
    table.add_column("AAPL EOD", eod_bars_to_close_column(apple))
    # table.add_column("AAPL EPS", fundamentals_column(apple, "EPS", "ARQ"), :most_recent_or_omit)
    arq_attribute_dimension_triples.each do |row|
      attribute_name = row[:attribute_name]
      attribute_label = row[:attribute_label]
      dimension_name = row[:dimension_name]
      table.add_column("AAPL #{attribute_name}", fundamentals_column(apple, attribute_label, dimension_name), :most_recent_or_omit)
    end
    puts table.to_csv(true, false)
  end
end
