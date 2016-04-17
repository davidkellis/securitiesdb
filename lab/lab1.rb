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

def main
  Application.load(ARGV.first || Application::DEFAULT_CONFIG_FILE_PATH)
  Lab1.new.build_table
end

main if __FILE__ == $0
