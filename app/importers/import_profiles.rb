require 'date'
require 'watir-webdriver'
require 'pp'
require_relative '../application'

class ProfileImporter
  def initialize
    @industries = {}
    @sectors = {}
  end

  def import_profiles(securities)
    browser = Watir::Browser.new :firefox
    downloader = ProfileDownloader.new(browser)

    for security in securities
      profile = downloader.extract_company_profile_from_morningstar(security.symbol)
      import_profile(security, profile) if profile
      security.reload
    end

    browser.quit
  end

  def import_profile(security, profile)
    #puts "Updating #{profile.symbol}"
    industry = find_or_create_industry(profile.industry)
    sector = find_or_create_industry(profile.sector)
    security.update(
        cik: profile.cik,
        fiscal_year_end_date: profile.fiscal_year_end,
        industry: industry,
        sector: sector
    )
  rescue => e
    puts "Unable to update profile: #{security.values.inspect}"
    puts ">> #{e.message}"
  end

  def find_or_create_industry(name)
    @industries[name] ||= Industry.first_or_create(:name => name)
  end

  def find_or_create_sector(name)
    @sectors[name] ||= Sector.first_or_create(:name => name)
  end
end

def main
  Database.connect

  securities = Stock.amex + Stock.nasdaq + Stock.nyse

  ProfileImporter.new.import_profiles(securities)
end

main if __FILE__ == $0