require 'pp'
require_relative 'application'

def main
  Application.load_config(ARGV.first || Application::DEFAULT_CONFIG_FILE_PATH)

  require_relative 'lib/bsym'

  # pp Bsym::Client.new.exchange_codes
  # pp Bsym::Client.new.security_types
  # puts Bsym::Client.new.predefined_files
  # pp Bsym::Client.new.mutual_funds.to_a
  # pp Bsym::Client.new.get_securities_from_predefined_file("Equity/Closed-End Fund").count
  # pp Bsym::Client.new.stocks.count
end

main
