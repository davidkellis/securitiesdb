require 'pp'
require_relative 'application'

def main
  Application.load(ARGV.first || Application::DEFAULT_CONFIG_FILE_PATH)

  # pp CsiData::Client.new.amex
  QuandlEod::Client.new(Application.logger).send(:enumerate_rowsets_in_csv) {|symbol, bars| puts symbol, bars.inspect; break }
end

main
