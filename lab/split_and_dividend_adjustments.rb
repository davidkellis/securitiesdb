require_relative '../application'

class SplitLab
  def run
    t1 = Time.now
    puts "Starting. #{t1}"

    netflix = FindSecurity.us_stocks.one("NFLX", 20150101)

    table = TimeSeriesTable.new

    table.add_column(Variables::EodBarClose.new(netflix).memoized(30))
    table.add_column(Variables::AdjustedEodBarClose.new(netflix, 20160101).memoized(30))
    table.add_column(Variables::EodBarVolume.new(netflix).memoized(30))
    table.add_column(Variables::AdjustedEodBarVolume.new(netflix, 20160101).memoized(30))


    business_days = Date.date_series_inclusive(
      Date.next_business_day(Date.datestamp_to_date(20150101)),
      Date.datestamp_to_date(20151231),
      ->(date) { Date.next_business_day(date) }
    ).map {|date| DateTime.to_timestamp(Date.date_at_time(date, 17, 0, 0)) }

    table.to_a(business_days).each do |row|
      puts row.inspect
    end

    t2 = Time.now
    puts "Finished. #{Time.now} ; #{t2 - t1} seconds"
  end
end

def main
  Application.load(ARGV.first || Application::DEFAULT_CONFIG_FILE_PATH)
  SplitLab.new.run
end

main if __FILE__ == $0
