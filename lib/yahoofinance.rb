#!/usr/bin/ruby
#
# YahooFinance ruby module.
# Homepage: http://www.transparentech.com/projects/yahoofinance
#
#   Ruby module for getting stock quote information from the
#   finance.yahoo.com website.
#
#   This module can be "required" and used as a library. Or it can be
#   run from the command-line as a script.
#
#
# Copyright (c) 2006 Nicholas Rahn <nick at transparentech.com>
#
# This software may be freely redistributed under the terms of the GNU
# public license version 2.
#
# This package is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See the "GNU General Public License" for more detail.
#

require 'net/http'
require 'csv'
require 'date'
require 'optparse'
require 'ostruct'

# Was using this to test a theory about why requests would hang
# randomly, but since upgrading to ruby 1.8.4, I haven't had many
# hangs.  I also tend to think that Yahoo has improved the reliability
# of this service a bit. My guess is that most of the hangs were due
# to dropped requests.  Also, catching the correct Exception in the
# #get method, I think, fixed most of the remaining problems.
#
#require 'timeout'


#
# Module for retrieving quote data ('live' and 'historical') from
# YahooFinance!
#
# This module is made up of 2 parts:
#   1. Retrieving 'live' quote data. 'Live' quotes are the current
# data available for a given stock.  Most often delayed 20mins.
#   2. Retrieving 'historical' quote data.  'Historical' quotes are
# for past pricings of a given stock.
#
module YahooFinance

  #
  # 'Live' quote data retrieval.
  #

  STDHASH = {
      "s" => ["symbol", "val"],
      "n" => ["name", "val"],
      "l1" => ["lastTrade", "val.to_f"],
      "d1" => ["date", "val"],
      "t1" => ["time", "val"],
      "c" => ["change", "val"],
      "c1" => ["changePoints", "val.to_f"],
      "p2" => ["changePercent", "val.to_f"],
      "p" => ["previousClose", "val.to_f"],
      "o" => ["open", "val.to_f"],
      "h" => ["dayHigh", "val.to_f"],
      "g" => ["dayLow", "val.to_f"],
      "v" => ["volume", "val.to_i"],
      "m" => ["dayRange", "val"],
      "l" => ["lastTradeWithTime", "val"],
      "t7" => ["tickerTrend", "convert(val)"],
      "a2" => ["averageDailyVolume", "val.to_i"],
      "b" => ["bid", "val.to_f"],
      "a" => ["ask", "val.to_f"]
      # These return integers like "1,000".  The CVS parser gets confused by this
      # so I've removed them for the time being.
      #    "b6" => [ "bidSize", "val" ],
      #    "a5" => [ "askSize", "val" ]
      #    "k3" => [ "lastTradeSize", "convert(val)" ],
  }

  EXTENDEDHASH = {
      "s" => ["symbol", "val"],
      "n" => ["name", "val"],
      "w" => ["weeks52Range", "val"],
      "j5" => ["weeks52ChangeFromLow", "val.to_f"],
      "j6" => ["weeks52ChangePercentFromLow", "val"],
      "k4" => ["weeks52ChangeFromHigh", "val.to_f"],
      "k5" => ["weeks52ChangePercentFromHigh", "val"],
      "e" => ["earningsPerShare", "val.to_f"],
      "r" => ["peRatio", "val.to_f"],
      "s7" => ["shortRatio", "val"],
      "r1" => ["dividendPayDate", "val"],
      "q" => ["exDividendDate", "val"],
      "d" => ["dividendPerShare", "convert(val)"],
      "y" => ["dividendYield", "convert(val)"],
      "j1" => ["marketCap", "convert(val)"],
      "t8" => ["oneYearTargetPrice", "val"],
      "e7" => ["epsEstimateCurrentYear", "val"],
      "e8" => ["epsEstimateNextYear", "val"],
      "e9" => ["epsEstimateNextQuarter", "val"],
      "r6" => ["pricePerEPSEstimateCurrentYear", "val"],
      "r7" => ["pricePerEPSEstimateNextYear", "val"],
      "r5" => ["pegRatio", "val.to_f"],
      "b4" => ["bookValue", "val.to_f"],
      "p6" => ["pricePerBook", "val.to_f"],
      "p5" => ["pricePerSales", "val.to_f"],
      "j4" => ["ebitda", "val"],
      "m3" => ["movingAve50days", "val"],
      "m7" => ["movingAve50daysChangeFrom", "val"],
      "m8" => ["movingAve50daysChangePercentFrom", "val"],
      "m4" => ["movingAve200days", "val"],
      "m5" => ["movingAve200daysChangeFrom", "val"],
      "m6" => ["movingAve200daysChangePercentFrom", "val"],
      "s1" => ["sharesOwned", "val"],
      "p1" => ["pricePaid", "val"],
      "c3" => ["commission", "val"],
      "v1" => ["holdingsValue", "val"],
      "w1" => ["dayValueChange", "val"],
      "g1" => ["holdingsGainPercent", "val"],
      "g4" => ["holdingsGain", "val"],
      "d2" => ["tradeDate", "val"],
      "g3" => ["annualizedGain", "val"],
      "l2" => ["highLimit", "val"],
      "l3" => ["lowLimit", "val"],
      "n4" => ["notes", "val"],
      "x" => ["stockExchange", "val"]
      # This returns an integer like "1,000,000".
      # The CVS parser gets confused by this
      # so I've removed it for the time being.
      #    "f6" => [ "floatShares", "val" ],
  }

  REALTIMEHASH = {
      "s" => ["symbol", "val"],
      "n" => ["name", "val"],
      "b2" => ["ask", "val.to_f"],
      "b3" => ["bid", "val.to_f"],
      "k2" => ["change", "val"],
      "k1" => ["lastTradeWithTime", "val"],
      "c6" => ["changePoints", "val.to_f"],
      "m2" => ["dayRange", "val"],
      "j3" => ["marketCap", "convert(val)"],
      #     "v7" => [ "holdingsValue", "val" ],
      #     "w4" => [ "dayValueChange", "val" ],
      #     "g5" => [ "holdingsGainPercent", "val" ],
      #     "g6" => [ "holdingsGain", "val" ],
      #     "r2" => [ "pe", "val" ],
      #     "c8" => [ "afterHoursChange", "val" ],
      #     "i5" => [ "orderBook", "val" ],
  }

  DEFAULT_READ_TIMEOUT = 5

  #
  # Return a string containing the results retrieved from
  # YahooFinance.  If there was an error during the execution of this
  # method, the string "" is returned.  In practice, this means that
  # no *Quote objects will be created (empty hashes will be returned
  # from get_*_quotes methods).
  #
  def YahooFinance.get(symbols, format, timeout=DEFAULT_READ_TIMEOUT)
    return "" if symbols == nil
    symbols = symbols.join(",") if symbols.class == Array
    symbols.strip!
    return "" if symbols == ""

    # Catch any exceptions that might occur here.  Possible exceptions
    # are the read_timeout and...
    #

    #
    # Don't catch any exceptions!  Just let them be thrown.
    #
    proxy = ENV['http_proxy'] ? URI.parse(ENV['http_proxy']) : OpenStruct.new
    Net::HTTP::Proxy(proxy.host, proxy.port,
                     proxy.user, proxy.password).start("download.finance.yahoo.com", 80) do |http|
      http.read_timeout = timeout
      response = nil
      response = http.get("/d/quotes.csv?s=#{symbols}&f=#{format}&e=.csv")
      return "" if !response
      response.body.chomp
    end
  end

  def YahooFinance.get_quotes(quote_class, symbols, &block)
    if quote_class == YahooFinance::StandardQuote
      return get_standard_quotes(symbols, &block)
    elsif quote_class == YahooFinance::ExtendedQuote
      return get_extended_quotes(symbols, &block)
    elsif quote_class == YahooFinance::RealTimeQuote
      return get_realtime_quotes(symbols, &block)
    else
      # Use the standard quote if the given quote_class was not recoginized.
      return get_standard_quotes(symbols, &block)
    end
  end

  def YahooFinance.get_realtime_quotes(symbols)
    csvquotes = YahooFinance::get(symbols, REALTIMEHASH.keys.join)
    ret = Hash.new
    CSV.parse(csvquotes) do |row|
      qt = RealTimeQuote.new(row)
      if block_given?
        yield qt
      end
      ret[qt.symbol] = qt
    end
    ret
  end

  def YahooFinance.get_extended_quotes(symbols)
    csvquotes = YahooFinance::get(symbols, EXTENDEDHASH.keys.join)
    ret = Hash.new
    CSV.parse(csvquotes) do |row|
      qt = ExtendedQuote.new(row)
      if block_given?
        yield qt
      end
      ret[qt.symbol] = qt
    end
    ret
  end

  def YahooFinance.get_standard_quotes(symbols)
    csvquotes = YahooFinance::get(symbols, STDHASH.keys.join)
    ret = Hash.new
    CSV.parse(csvquotes) do |row|
      qt = StandardQuote.new(row)
      if block_given?
        yield qt
      end
      ret[qt.symbol] = qt
    end
    ret
  end

  class BaseQuote
    def initialize(hash, valarray=nil)
      @formathash = hash
      @formathash.each_key { |elem|
        # Create a getter method for each format element.
        instance_eval("def #{@formathash[elem][0]}() " +
                          "@#{@formathash[elem][0]} " +
                          "end")
        # Create a setter method for each format element.
        instance_eval("def #{@formathash[elem][0]}=(val) " +
                          "@#{@formathash[elem][0]}=#{@formathash[elem][1]} " +
                          "end")
      }

      parse(valarray) if valarray
    end

    def load_quote(symbol)
      csv = YahooFinance.get(symbol, @formathash.keys.join)
      parse(CSV.parse_line(csv))
    end

    def valid?()
      # Not sure this is the best way to do this, but OK for now.
      return self.name != self.symbol if self.name
      false
    end

    def get_info()
      "#{symbol} : #{name}"
    end

    def to_s()
      ret = String.new
      ret << self.class.name << "\n"
      @formathash.each_value { |val|
        ret << "#{val[0]} = "
        ret << send(val[0]).to_s unless send(val[0]) == nil
        ret << "\n"
      }
      return ret
    end

    protected

    def parse(results)
      begin
        ctr = 0
        results.each { |elem|
          # Call the setter method for this element.
          send "#{@formathash[@formathash.keys[ctr]][0]}=", elem
          ctr += 1
        }
      rescue
        puts "yfparse:#{$!}"
      end
    end

    def convert(value)
      if (value == "N/A")
        return value
      elsif (value =~ /.*\..*B/)
        return value
      else
        return value
      end
    end

  end

  class RealTimeQuote < YahooFinance::BaseQuote
    def initialize(valarray=nil)
      super(YahooFinance::REALTIMEHASH, valarray)
    end
  end

  class ExtendedQuote < YahooFinance::BaseQuote
    def initialize(valarray=nil)
      super(YahooFinance::EXTENDEDHASH, valarray)
    end
  end

  class StandardQuote < YahooFinance::BaseQuote
    def initialize(valarray=nil)
      super(YahooFinance::STDHASH, valarray)
    end

    def get_info()
      "#{symbol} : #{lastTrade} : #{changePoints} (#{changePercent})"
    end
  end

  #
  # 'Historical' quote retrieval.
  #

  class HistoricalQuote
    attr_accessor :recno, :symbol, :date, :open, :high, :low, :close, :adjClose, :volume

    def initialize(sym, valarray=nil, &block)
      @symbol = sym.upcase
      if valarray

        @date = HistoricalQuote.parse_date(valarray[0])
        @open = valarray[1].to_f
        @high = valarray[2].to_f
        @low = valarray[3].to_f
        @close = valarray[4].to_f
        @volume = valarray[5].to_i
        @adjClose = valarray[6].to_f
        @recno = valarray[7].to_i if valarray.size >= 8
      end
      if block_given?
        instance_eval(&block)
      end
    end

    @@date_re = /([0-9]{1,2})-([A-Za-z]+)-([0-9]{1,2})/
    @@months = %w( Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec )

    #
    # This method is obsolete since the Yahoo format change.  However,
    # I am leaving it here for API stability.
    #
    def HistoricalQuote.parse_date(date)
      # Yahoo changed the format of the date column. It is now in the
      # same format as this method returned, so this method just
      # returns the same date.
      return date
      #       md = @@date_re.match( date )
      #       if md
      #         if md[3].to_i > 30
      #           year = "19#{md[3]}"
      #         else
      #           year = "20#{md[3]}"
      #         end
      #         return "#{year}-%02d-%02d" % [(@@months.index(md[2]) + 1), md[1].to_i]
      #       else
      #         return date
      #       end
    end

    def HistoricalQuote.parse_date_to_Date(date)
      return Date.parse(parse_date(date))
    end

    def to_s
      return "#{symbol},#{date},#{open},#{high},#{low},#{close},#{volume},#{adjClose}"
    end

    def to_a
      return [] << symbol << date << open << high <<
          low << close << volume << adjClose
    end

    def date_to_Date
      return Date.parse(@date)
    end
  end

  def YahooFinance.retrieve_raw_historical_quotes(symbol, startDate, endDate)
    # Don't try to download anything if the starting date is before the end date.
    return [] if startDate > endDate

    proxy = ENV['http_proxy'] ? URI.parse(ENV['http_proxy']) : OpenStruct.new
    Net::HTTP::Proxy(proxy.host,
                     proxy.port,
                     proxy.user,
                     proxy.password).start("ichart.finance.yahoo.com", 80) { |http|
      #Net::HTTP.start( "ichart.finance.yahoo.com", 80 ) { |http|
      query = "/table.csv?s=#{symbol}&g=d&a=#{startDate.month-1}&b=#{startDate.mday}&c=#{startDate.year}&d=#{endDate.month-1}&e=#{endDate.mday}&f=#{endDate.year.to_s}"
      #puts "#{query}"
      response = http.get(query)
      #puts "#{response.body}"
      body = response.body.chomp

      # If we don't get the first line like this, there was something
      # wrong with the data (404 error, new data formet, etc).
      return [] if body !~ /Date,Open,High,Low,Close,Volume,Adj Close/

      # Parse into an array of arrays.
      rows = CSV.parse(body)
      # Remove the first array since it is just the field headers.
      rows.shift
      #puts "#{rows.length}"

      return rows
    }
  end

  def YahooFinance.retrieve_raw_historical_dividends(symbol, startDate, endDate)
    # Don't try to download anything if the starting date is before the end date.
    return [] if startDate > endDate

    proxy = ENV['http_proxy'] ? URI.parse(ENV['http_proxy']) : OpenStruct.new
    Net::HTTP::Proxy(proxy.host,
                     proxy.port,
                     proxy.user,
                     proxy.password).start("ichart.finance.yahoo.com", 80) { |http|
      query = "/table.csv?s=#{symbol}&g=v&a=#{startDate.month-1}&b=#{startDate.mday}&c=#{startDate.year}&d=#{endDate.month-1}&e=#{endDate.mday}&f=#{endDate.year.to_s}"
      response = http.get(query)
      #puts "#{response.body}"
      body = response.body.chomp

      # If we don't get the first line like this, there was something
      # wrong with the data (404 error, new data formet, etc).
      return [] if body !~ /Date,Dividends/

      # Parse into an array of arrays.
      rows = CSV.parse(body)
      # Remove the first array since it is just the field headers.
      rows.shift
      #puts "#{rows.length}"

      return rows
    }
  end

  def YahooFinance.retrieve_raw_historical_dividends_and_splits(symbol, startDate, endDate)
    # Don't try to download anything if the starting date is before the end date.
    return [] if startDate > endDate

    proxy = ENV['http_proxy'] ? URI.parse(ENV['http_proxy']) : OpenStruct.new
    Net::HTTP::Proxy(proxy.host,
                     proxy.port,
                     proxy.user,
                     proxy.password).start("ichart.finance.yahoo.com", 80) { |http|
      query = "/x?s=#{symbol}&g=v&a=#{startDate.month-1}&b=#{startDate.mday}&c=#{startDate.year}&d=#{endDate.month-1}&e=#{endDate.mday}&f=#{endDate.year.to_s}"
      response = http.get(query)
      #puts "#{response.body}"
      body = response.body.chomp

      # If we don't get the first line like this, there was something
      # wrong with the data (404 error, new data formet, etc).
      return [] if body !~ /Date,Dividends/

      # body will look like this:
      # Date,Dividends
      # DIVIDEND, 20121219,0.585000
      # DIVIDEND, 20120925,0.315000
      # SPLIT, 20050609,2:1
      # DIVIDEND, 20000920,0.102000
      # STARTDATE, 20000526
      # ENDDATE, 20121231
      # TOTALSIZE, 44
      # STATUS, 0

      # Parse into an array of arrays.
      body = body.gsub(', ', ',')   # remove the spaces after each comma delimiter
      rows = CSV.parse(body)
      # Remove the first array since it is just the field headers.
      rows.shift

      return rows.select {|row| first_col = row.first.to_s.strip; first_col == "DIVIDEND" || first_col == "SPLIT" }
    }
  end

  def self.get_dividends_and_splits(symbol, start_date, end_date)
    rows = retrieve_raw_historical_dividends_and_splits(symbol, start_date, end_date)
    rows.reduce({:dividends => [], :splits => []}) do |memo,row|
      case row.first.downcase
      when "dividend"
        memo[:dividends] << row.drop(1)
      when "split"
        memo[:splits] << row.drop(1)
      end
      memo
    end
  end

  def YahooFinance.get_historical_quotes(symbol, startDate, endDate)
    rows = []
    rowct = 0
    gotchunk = false

    #
    # Yahoo is only able to provide 200 data points at a time for Some
    # "international" markets.  We want to download all of the data
    # points between the startDate and endDate, regardless of how many
    # 200 data point chunks it comes in.  In this section we download
    # as many chunks as needed to get all of the data points.
    #
    begin
      gotchunk = false
      r = YahooFinance.retrieve_raw_historical_quotes(symbol, startDate, endDate)
      if block_given?
        # If we were given a block, yield to it for every row of data
        # downloaded.
        r.each { |row| yield row }
      else
        rows += r
      end

      # Is this a chunk?
      if r.length == 200
        # Adjust the endDate for when we retrieve the next chunk.
        endDate = HistoricalQuote.parse_date_to_Date(r.last[0]) - 1
        # Marke this as a chunk so do the download again with the new endDate.
        gotchunk = true
      end

    end while gotchunk

    if block_given?
      # If we have already yielded to every row, just return nil.
      return nil
    else
      # Otherwise return the big array of arrays.
      rows
    end
  end

  def YahooFinance.get_historical_quotes_days(symbol, days, &block)
    endDate = Date.today()
    startDate = Date.today() - days
    YahooFinance.get_historical_quotes(symbol, startDate, endDate, &block)
  end

  def YahooFinance.get_historical_dividends_days(symbol, days, &block)
    endDate = Date.today()
    startDate = Date.today() - days

    rows = YahooFinance.retrieve_raw_historical_dividends(symbol, startDate, endDate)

    if block_given?
      # If we were given a block, yield to it for every row of data
      # downloaded.
      rows.each { |row| yield row }
      nil
    else
      rows
    end
  end

  def YahooFinance.get_HistoricalQuotes(symbol, startDate, endDate)
    ret = []
    YahooFinance.get_historical_quotes(symbol, startDate, endDate) { |row|
      if block_given?
        yield HistoricalQuote.new(symbol, row)
      else
        ret << HistoricalQuote.new(symbol, row)
      end
    }
    if block_given?
      return nil
    else
      return ret
    end
  end

  def YahooFinance.get_HistoricalQuotes_days(symbol, days, &block)
    endDate = Date.today()
    startDate = Date.today() - days
    YahooFinance.get_HistoricalQuotes(symbol, startDate, endDate, &block)
  end
end


if $0 == __FILE__

  class CLIOpts
    def self.help(options, opts)
      puts "ERROR: #{options.helpMsg}" if options.helpMsg
      puts opts
      exit
    end

    # Return a structure describing the options.
    def self.parse(args, options)

      opts = OptionParser.new do |opts|
        opts.banner = "Usage: yahoofinance.rb [options] <symbol>"

        opts.separator ""

        opts.on("-s", "Retrieve standard quotes (default).") {
          options.quote_class = YahooFinance::StandardQuote
        }
        opts.on("-x", "Retrieve extended quotes.") {
          options.quote_class = YahooFinance::ExtendedQuote
        }
        opts.on("-r", "Retrieve real-time quotes.") {
          options.quote_class = YahooFinance::RealTimeQuote
        }
        opts.on('-z', "Retrieve historical quotes.") {
          options.quote_class = nil
        }
        opts.on("-d", "--days N", Integer, "Number of days of historical " +
            "quotes to retrieve. Default is 90.") { |days|
          options.historical_days = days
        }
        opts.on("-h", "--help", "Show this message") do
          options.help = true
        end

      end

      begin
        opts.parse!(args)
      rescue OptionParser::InvalidOption
        options.help = true
        options.helpMsg = $!.message
        help(options, opts)
      end

      if options.help
        help(options, opts)
      end

      if args.length > 0 && options.help != true
        options.symbol = args[0]
      else
        options.help = true
        options.helpMsg = "Missing Symbol!"
        help(options, opts)
      end
    end
  end

  $options = OpenStruct.new
  $options.help = false
  $options.helpMsg = nil
  $options.symbol = nil
  $options.quote_class = YahooFinance::StandardQuote
  $options.historical_days = 90

  CLIOpts.parse(ARGV, $options)
  puts "Retrieving quotes for: #{$options.symbol}"

  if $options.quote_class
    YahooFinance::get_quotes($options.quote_class, $options.symbol) do |qt|
      puts "QUOTING: #{qt.symbol}"
      #puts "#{qt.get_info}"
      puts qt.to_s
    end
  else
    $options.symbol.split(',').each do |s|
      YahooFinance::get_historical_quotes_days(s, $options.historical_days) do
      |row|
        puts "#{s},#{row.join(',')}"
      end
    end
  end

end
