require 'pp'
require 'watir-webdriver'
require 'fileutils'

BASE_URI = "http://financials.morningstar.com/balance-sheet/bs.html?t=<TICKER>&region=USA&culture=en-us"

REPORT_TYPES = {
    :annual => "Annual",
    :quarterly => "Quarterly"
}

def open_browser(download_directory = nil)
  download_directory ||= "#{Dir.pwd}/balance_sheets"
  download_directory.gsub!("/", "\\") if  Selenium::WebDriver::Platform.windows?

  profile = Selenium::WebDriver::Chrome::Profile.new
  profile['download.prompt_for_download'] = false
  profile['download.default_directory'] = download_directory

  Watir::Browser.new :chrome, :profile => profile
end

# report_type must be either :annual or :quarterly
def download_balance_sheets_from_morningstar(tickers, report_type = :annual)
  puts "Downloading balance sheet history from Morningstar."

  b = open_browser("#{Dir.pwd}/#{report_type}_balance_sheets")

  tickers.each do |ticker|
    puts "Processing #{ticker}"

    begin
      # open the balance sheet page for <ticker>
      uri = BASE_URI.gsub("<TICKER>", ticker)
      b.goto uri

      # select the type of report we want to download - annual or quarterly
      dropdown_menu = b.li :id => "menu_A"
      dropdown_menu.click
      report_type_link = dropdown_menu.link :text => REPORT_TYPES[report_type]
      report_type_link.click

      # locate the button for rounding the figures down one order of magnitude
      round_down_button = b.link :class => "rf_rounddn"

      # click the round-down button until we can't click it anymore - that means we've rounded down as far as possible
      while round_down_button.exists?
        round_down_button.click
      end

      # download the balance sheet as a CSV file
      export_button = b.link :class => "rf_export"
      export_button.click
    rescue Exception => e
      puts "Error while processing #{ticker} - #{e.message}"
      next
    end
  end

  b.quit
end

def main
  tickers = if ARGV.length == 0
              %w{AAPL F VFINX}
            elsif ARGV.length == 1 && ARGV[0].index(/\.\w+/)   # treat as filename
              File.readlines(ARGV[0]).map{|line| line.strip }
            else
              ARGV
            end

  report_type = :annual

  FileUtils.mkdir("#{report_type}_balance_sheets") unless File.exists?("#{report_type}_balance_sheets")

  # grab only tickers that are traded on primary american exchanges - those without an exchange suffix (e.g. ticker.OB)
  tickers = tickers.select {|ticker| ticker.index(".").nil? }
  puts "#{tickers.count} symbols"

  download_balance_sheets_from_morningstar(tickers, report_type)
end

main
