require 'watir-webdriver'
require 'pp'

class ProfileDownloader

  Profile = Struct.new(:symbol, :sector, :industry, :cik, :fiscal_year_end)

  BASE_URI = "http://financials.morningstar.com/company-profile/c.action?t=<TICKER>&region=USA&culture=en-US"

  attr_accessor :browser

  def initialize(browser)
    self.browser = browser
  end

  def extract_company_profile_from_morningstar(ticker)
    # open the profile page for <ticker>
    uri = BASE_URI.gsub("<TICKER>", ticker)
    browser.goto uri


    # extract data from the business description table
    basic_data_table = browser.table(:xpath => '//*[@id="BasicData"]/table').tap(&:wait_until_present)
    sector_and_industry_row = basic_data_table.tbody.rows[5]
    sector = sector_and_industry_row.cells[2].text
    industry = sector_and_industry_row.cells[4].text


    # extract data from the operation details table
    operation_details_table = browser.table(:xpath => '//*[@id="OperationDetails"]/table').tap(&:wait_until_present)
    fiscal_year_end_row = operation_details_table.tbody.rows[0]
    cik_row = operation_details_table.tbody.rows[2]
    fiscal_year_end = fiscal_year_end_row.cells[1].text
    cik = cik_row.cells[1].text

    fiscal_year_end = reformat_date(fiscal_year_end)
    cik = convert_cik(cik)

    Profile.new(ticker, sector, industry, cik, fiscal_year_end)
  rescue Exception => e
    puts "Error while processing #{ticker}: #{e.message}"
    nil
  end

  def convert_cik(cik)
    cik.to_i if cik
  end

  # converts a date in yyyy-mm-dd format to yyyymmdd format
  def reformat_date(date)
    date.gsub("-", "").to_i if date
  end

end

def main
  browser = Watir::Browser.new :firefox
  pp ProfileDownloader.new(browser).extract_company_profile_from_morningstar("AAPL")
  browser.quit
end

main if __FILE__ == $0