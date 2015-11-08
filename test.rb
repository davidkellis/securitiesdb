require 'pp'
require_relative 'application'

def main
  Application.load_config(ARGV.first || Application::DEFAULT_CONFIG_FILE_PATH)

  require_relative 'lib/bsym'

  # pp Bsym::Client.new.exchange_codes
  # pp Bsym::Client.new.security_types
  puts Bsym::Client.new.predefined_files.values.all? {|url| url.end_with?("20151107.txt.zip") }
end

main
