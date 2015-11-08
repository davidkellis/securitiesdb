require 'pp'
require 'nokogiri'
require 'open-uri'

BASE_URI = "http://biz.yahoo.com"

def extract_tickers(uri)
  puts "Processing #{uri}"
  doc = Nokogiri::HTML(open(uri))

  tickers = doc.xpath('/html/body//td/tt').map do |ticker|
    ticker.content.strip
  end
end

def extract_root_uris
  root_uri = 'http://biz.yahoo.com/i/'
  doc = Nokogiri::HTML(open(root_uri))

  # /html/body/center[3]/p[4]/table/tbody/tr
  table = doc.css('body table')[2]

  links = table.css('tr > td > a')

  uri_paths = links.map{|a_tag| a_tag['href'].to_s }

  uri_paths.map{|path| "#{BASE_URI}#{path}" }
end

def main
  root_uris = extract_root_uris

  all_tickers = []
  root_uris.each do |uri|
    puts "Processing #{uri}"
    doc = Nokogiri::HTML(open(uri))

    # Search for nodes by xpath
    sibling_page_uris = doc.xpath('/html/body/center[3]//table[3]//td[@nowrap]/a').map do |link|
      "#{BASE_URI}#{link["href"].strip}"
    end

    # grab tickers from first page
    tickers = doc.xpath('/html/body//td/tt').map do |ticker|
      ticker.content.strip
    end
    all_tickers << tickers

    # grab tickers from all subsequent sister pages
    sibling_page_uris.each do |uri|
      all_tickers << extract_tickers(uri)
    end
  end

  all_tickers = all_tickers.flatten.uniq

  puts all_tickers.count

  File.open("all_tickers.txt", "w+") do |f|
    f.write all_tickers.join("\n")
  end
end

main