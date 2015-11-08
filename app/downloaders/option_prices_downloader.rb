require "bundler/setup"
Bundler.require(:default)

require 'pp'
require 'thread'
require 'open-uri'

ENV['MONGOID_ENV'] = "development"
Mongoid.load!("mongoid.yml")

class OptionQuote
  include Mongoid::Document

  field :time, type: String
  field :type, type: Symbol
  field :strike, type: BigDecimal
  field :symbol, type: String
  field :last, type: BigDecimal
  field :change, type: BigDecimal
  field :bid, type: BigDecimal
  field :ask, type: BigDecimal
  field :volume, type: BigDecimal
  field :open_interest, type: BigDecimal
end

class YahooOptionsReader
  TICKER_PLACEHOLDER            = "<TICKER>"
  BASE_URI                      = "http://finance.yahoo.com"
  OPTIONS_HOME_URI              = "http://finance.yahoo.com/q/op?s=<TICKER>"
  ALTERNATE_PAGE_LINKS_XPATH    = "//table[@id='yfncsumtab']/tr[2]/td/a"
  CALL_OPTIONS_TABLE_ROWS_XPATH = "//table[@id='yfncsumtab']/tr[2]/td/table[@class='yfnc_datamodoutline1'][1]/tr/td/table/tr[position()>1]"
  PUT_OPTIONS_TABLE_ROWS_XPATH  = "//table[@id='yfncsumtab']/tr[2]/td/table[@class='yfnc_datamodoutline1'][2]/tr/td/table/tr[position()>1]"

  attr_accessor :ticker

  def initialize(ticker)
    self.ticker = ticker
  end

  def get_uri(uri)
    Nokogiri::HTML(open(uri))
  end

  def primary_quote_page
    uri = OPTIONS_HOME_URI.gsub(TICKER_PLACEHOLDER, ticker)
    get_uri(uri)
  end

  def links_to_alternate_expiration_months(primary_page)
    uris = primary_page.xpath(ALTERNATE_PAGE_LINKS_XPATH).map do |link|
      "#{BASE_URI}#{link["href"].strip}"
    end
  end

  def alternate_quote_pages(primary_page)
    links_to_alternate_expiration_months(primary_page).map {|uri| get_uri(uri) }
  end

  def options_from_page(page)
    call_option_rows = page.xpath(CALL_OPTIONS_TABLE_ROWS_XPATH)

    put_option_rows = page.xpath(PUT_OPTIONS_TABLE_ROWS_XPATH)

    call_quotes = call_option_rows.map {|row| extract_option_quote_from_row(row, :call) }
    put_quotes = put_option_rows.map {|row| extract_option_quote_from_row(row, :put) }

    call_quotes.concat(put_quotes)
  end

  def extract_option_quote_from_row(row, option_type)
    td_cells = row.xpath("./td")

    time = Time.now.strftime("%Y%m%d%H%M%S")
    strike = td_cells[0].text.strip
    symbol = td_cells[1].text.strip
    last = td_cells[2].text.strip
    change = td_cells[3].text.strip
    bid = td_cells[4].text.strip
    ask = td_cells[5].text.strip
    volume = td_cells[6].text.strip
    open_interest = td_cells[7].text.strip
    
    OptionQuote.new(time: time,
                    type: option_type, 
                    strike: strike, 
                    symbol: symbol, 
                    last: last, 
                    change: change, 
                    bid: bid, 
                    ask: ask, 
                    volume: volume, 
                    open_interest: open_interest)
  end

  def quotes
    puts "Retrieving quotes for #{ticker} at #{Time.now}"

    primary_page = primary_quote_page
    alternate_pages = alternate_quote_pages(primary_page)

    option_quotes = [primary_page].concat(alternate_pages).map do |page|
      options_from_page(page)
    end

    option_quotes.flatten
  end
end

class PollingQuoteFeed
  EXCHANGE_NAME = "options.updates"

  attr_accessor :tickers
  attr_accessor :polling_interval

  def initialize(tickers, polling_interval)
    self.tickers = tickers
    self.polling_interval = polling_interval
    
    scheduler = Rufus::Scheduler.start_new
    
    @bunny = Bunny.new(:logging => true)
    @bunny.start
    setup_pub_sub_exchange
  end

  def setup_pub_sub_exchange
    @exchange = @bunny.exchange(EXCHANGE_NAME, :type => :fanout, :auto_delete => true)
  end

  def start
    EM.run do
      scheduler = Rufus::Scheduler::EmScheduler.start_new

      scheduler.every polling_interval, :allow_overlapping => false do
        puts "Starting scheduled job, refresh_quotes, at #{Time.now}"
        refresh_quotes
      end
    end
  end

  def refresh_quotes
    pool = Pool.new(2)
    tickers.each do |ticker|
      pool.schedule do
        quotes = retrieve_quotes(ticker)
        enqueue_notification(ticker) unless quotes.empty?
      end
    end
    pool.shutdown
  end

  def retrieve_quotes(ticker)
    download_quotes(ticker).each(&:save!)
  end

  def download_quotes(ticker)
    YahooOptionsReader.new(ticker).quotes
  end

  def enqueue_notification(ticker)
    @exchange.publish(ticker)
  end
end

def main
  tickers = if ARGV.length == 0
              %w{AAPL F}
            elsif ARGV.length == 1 && ARGV[0].index(/\.\w+/)   # treat as filename
              File.readlines(ARGV[0]).map{|line| line.strip }
            else
              ARGV
            end

  # grab only tickers that are traded on american exchanges - those without an exchange suffix (e.g. ticker.OB)
  tickers = tickers.select {|ticker| ticker.index(".").nil? }
  puts "#{tickers.count} symbols"

  PollingQuoteFeed.new(tickers, "30s").start

  # t1 = Time.now
  # YahooOptionsReader.new(tickers.first).quotes
  # t2 = Time.now
  # pp t2 - t1
end

main