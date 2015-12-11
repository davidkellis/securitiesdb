# This library makes the various contents of the CSI Data (http://www.csidata.com/) website available through a nice API.

module CsiData

  Security = Struct.new(
    :csi_number,
    :symbol,
    :name,
    :exchange,
    :is_active,
    :start_date,
    :end_date,
    :sector,
    :industry,
    :conversion_factor,
    :switch_cf_date,
    :pre_switch_cf,
    :last_volume,
    :type,
    :child_exchange,
    :currency
  )

  class Client
    CSV_URLS = {
      amex: "http://www.csidata.com/factsheets.php?type=stock&format=csv&exchangeid=80",
      nyse: "http://www.csidata.com/factsheets.php?type=stock&format=csv&exchangeid=79",
      nasdaq_otc: "http://www.csidata.com/factsheets.php?type=stock&format=csv&exchangeid=88",
      etf: "http://www.csidata.com/factsheets.php?type=stock&format=csv&isetf=1",
      etn: "http://www.csidata.com/factsheets.php?type=stock&format=csv&isetn=1",
      mutual_fund: "http://www.csidata.com/factsheets.php?type=stock&format=csv&exchangeid=85",
      us_stock_indices: "http://www.csidata.com/factsheets.php?type=stock&format=csv&exchangeid=81"
    }

    SYMBOL_LISTING_HEADER = "CsiNumber,Symbol,Name,Exchange,IsActive,StartDate,EndDate,Sector,Industry,ConversionFactor,SwitchCfDate,PreSwitchCf,LastVolume,Type,ChildExchange,Currency"

    def amex
      get_securities_of_type(:amex)
    end

    def nyse
      get_securities_of_type(:nyse)
    end

    def nasdaq_otc
      get_securities_of_type(:nasdaq_otc)
    end

    def etfs
      get_securities_of_type(:etf)
    end

    def etns
      get_securities_of_type(:etn)
    end

    def mutual_funds
      get_securities_of_type(:mutual_fund)
    end

    def us_stock_indices
      get_securities_of_type(:us_stock_indices)
    end

    # all stocks
    def stocks
      amex + nyse + nasdaq_otc
    end

    # all ETPs
    def etps
      etfs + etns
    end

    # security_type is one of the keys from the CSV_URLS hash
    def get_securities_of_type(security_type)
      url = CSV_URLS[security_type]
      if url
        get_securities(url)
      else
        raise "Unknown security CSV file type: #{security_type}"
      end
    end

    def get_securities(url)
      csv_contents = Net::HTTP.get(URI(url))
      csv_contents.encode!("UTF-8", "ISO-8859-1")   # CSI Data encodes their CSV files with the ISO-8859-1 character set, so we need to convert it to UTF-8
      rows = CSV.parse(csv_contents, headers: false, return_headers: false, skip_lines: /^(\s*,\s*)*$/)
      if rows.first.join(",") == SYMBOL_LISTING_HEADER
        rows.drop(1).map {|row| Security.new(*row.map{|s| s && s.strip }) }
      else
        raise "The securities list in #{url} doesn't conform to the expected row structure of: #{SYMBOL_LISTING_HEADER}."
      end
    end
  end

end
