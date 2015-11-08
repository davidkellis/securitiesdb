require 'watir-webdriver'

module Browser
  class << self
    def open(download_directory = Dir.pwd)
      download_directory.gsub!("/", "\\") if  Selenium::WebDriver::Platform.windows?

      profile = Selenium::WebDriver::Firefox::Profile.new
      profile['browser.download.dir'] = download_directory
      profile['browser.download.folderList'] = 2    # When set to 2, the location specified for the most recent download is utilized again.
      profile['browser.helperApps.neverAsk.saveToDisk'] = "application/octet-stream"
      Watir::Browser.new :firefox, :profile => profile

      # profile = Selenium::WebDriver::Chrome::Profile.new
      # profile['download.prompt_for_download'] = false
      # profile['download.default_directory'] = download_directory
      # Watir::Browser.new :chrome, :profile => profile
    end
  end
end
