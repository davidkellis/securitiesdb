require_relative '../application'

class Lab1
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

  def run
    t1 = Time.now
    puts "Starting. #{t1}"

    apple = FindSecurity.us_stocks.one("AAPL", 20150101)
    google = FindSecurity.us_stocks.one("GOOG", 20150101)
    microsoft = FindSecurity.us_stocks.one("MSFT", 20150101)
    exxon = FindSecurity.us_stocks.one("XOM", 20150101)
    ge = FindSecurity.us_stocks.one("GE", 20150101)


    business_days = Date.date_series_inclusive(
      Date.next_business_day(Date.datestamp_to_date(20150101)),
      Date.datestamp_to_date(20151231),
      ->(date) { Date.next_business_day(date) }
    ).map {|date| DateTime.to_timestamp(Date.date_at_time(date, 17, 0, 0)) }

    table = TimeSeriesTable.new
    table.add_column(Variables::EodBarClose.new(apple))
    table.add_column(Variables::EodBarClose.new(google))
    table.add_column(Variables::EodBarClose.new(microsoft))
    table.add_column(Variables::EodBarClose.new(exxon))
    table.add_column(Variables::EodBarClose.new(ge))

    # # table.add_column("AAPL EPS", fundamentals_column(apple, "EPS", "ARQ"), :most_recent_or_omit)
    # arq_attribute_dimension_triples.each do |row|
    #   attribute_name = row[:attribute_name]
    #   attribute_label = row[:attribute_label]
    #   dimension_name = row[:dimension_name]
    #   table.add_column("AAPL #{attribute_name}", fundamentals_column(apple, attribute_label, dimension_name), :most_recent_or_omit)
    # end

    pp table.to_a(business_days)
    # puts table.save_csv("lab1.csv", business_days, true, false)

    t2 = Time.now
    puts "Finished. #{Time.now} ; #{t2 - t1} seconds"
  end
end

def main
  Application.load(ARGV.first || Application::DEFAULT_CONFIG_FILE_PATH)
  Lab1.new.run
end

main if __FILE__ == $0
