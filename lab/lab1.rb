require_relative '../application'

class Lab1
  using DateExtensions

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
    apple = FindSecurity.us_stocks.one("AAPL")
    google = FindSecurity.us_stocks.one("GOOGL")
    microsoft = FindSecurity.us_stocks.one("MSFT")
    exxon = FindSecurity.us_stocks.one("XOM")
    ge = FindSecurity.us_stocks.one("GE")
    jnj = FindSecurity.us_stocks.one("JNJ")
    amazon = FindSecurity.us_stocks.one("AMZN")
    wellsfargo = FindSecurity.us_stocks.one("WFC")
    berkshire_hathaway = FindSecurity.us_stocks.one("BRK.B")
    jpmorgan = FindSecurity.us_stocks.one("JPM")

    xiv = FindSecurity.us_stocks.one("XIV")
    vxx = FindSecurity.us_stocks.one("VXX")

    vix = FindSecurity.us_indices.run("VIX Index")
    sp500 = FindSecurity.us_indices.run("SPX Index")


    arq_attribute_dimension_triples = identify_arq_fundamentals_tracked_for(apple)
    inst_attribute_dimension_triples = identify_inst_fundamentals_tracked_for(apple)

    business_days = Date.date_series_inclusive(
      Date.next_business_day(Date.datestamp_to_date(20150101)),
      Date.datestamp_to_date(20151231),
      ->(date) { Date.next_business_day(date) }
    ).map(&:to_datestamp)

    table = TimeSeriesTable.new
    table.add_column(Variables::EodBarClose.new(apple))
    # table.add_column("AAPL EPS", fundamentals_column(apple, "EPS", "ARQ"), :most_recent_or_omit)
    arq_attribute_dimension_triples.each do |row|
      attribute_name = row[:attribute_name]
      attribute_label = row[:attribute_label]
      dimension_name = row[:dimension_name]
      table.add_column("AAPL #{attribute_name}", fundamentals_column(apple, attribute_label, dimension_name), :most_recent_or_omit)
    end
    puts table.save_csv("lab1.csv", business_days, true, false)
  end
end

def main
  Application.load(ARGV.first || Application::DEFAULT_CONFIG_FILE_PATH)
  Lab1.new.run
end

main if __FILE__ == $0
