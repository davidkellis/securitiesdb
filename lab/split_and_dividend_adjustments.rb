require_relative '../application'

class SplitLab
  def run
    t1 = Time.now
    puts "Starting. #{t1}"

    apple = FindSecurity.us_stocks.one("AAPL", 20150101)

    t2 = Time.now
    puts "Finished. #{Time.now} ; #{t2 - t1} seconds"
  end
end

def main
  Application.load(ARGV.first || Application::DEFAULT_CONFIG_FILE_PATH)
  SplitLab.new.run
end

main if __FILE__ == $0
