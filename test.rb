require 'pp'
require_relative 'application'

def main
  Application.load(ARGV.first || Application::DEFAULT_CONFIG_FILE_PATH)

  # require_relative 'app/clients/bsym'
  # require_relative 'app/clients/csidata'

  # pp Bsym::Client.new.exchange_codes
  # pp Bsym::Client.new.security_types
  # pp Bsym::Client.new.security_types.count
  # pp Bsym::Client.new.predefined_files
  # pp Bsym::Client.new.mutual_funds.to_a
  # pp Bsym::Client.new.get_securities_from_predefined_file("Equity/Closed-End Fund").count
  # pp Bsym::Client.new.get_securities_from_predefined_file("Commodity/Financial commodity future.").to_a
  # pp Bsym::Client.new.stocks.first

  # pp CsiData::Client.new.amex
  QuandlEod::Client.new.send(:enumerate_rowsets_in_csv) {|symbol, bars| puts symbol, bars.inspect; break }
end

main
