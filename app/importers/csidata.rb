require 'date'
require 'io/console'
require 'pp'

require_relative "../clients/csidata"

class CsiDataImporter
  UNKNOWN_INDUSTRY_NAME = "Unknown"
  UNKNOWN_SECTOR_NAME = "Unknown"
  UNKNOWN_SECURITY_TYPE = "Unknown"
  APPROXIMATE_SEARCH_THRESHOLD = 0.7

  # this is a mapping from [CSI Exchange, CSI Child Exchange] to exchange label as defined in ExchangesImporter
  CSI_EXCHANGE_PAIR_TO_EXCHANGE_LABEL_MAP = {
    ["NYSE", nil] => "NYSE",
    ["AMEX", nil] => "NYSE-MKT",
    ["NYSE", "NYSE"] => "NYSE",
    ["OTC", nil] => "OTC",
    ["AMEX", "AMEX"] => "NYSE-MKT",
    ["OTC", "OTC Markets Pink Sheets"] => "OTC-PINK",
    ["OTC", "OTC Markets QX"] => "OTC-QX",
    ["OTC", "Nasdaq Global Select"] => "NASDAQ-GSM",
    ["OTC", "Nasdaq Global Market"] => "NASDAQ-GM",
    ["OTC", "OTC Markets QB"] => "OTC-QB",
    ["OTC", "Nasdaq Capital Market"] => "NASDAQ-CM",
    ["MUTUAL", "Mutual Fund"] => "MUTUAL",
    ["MUTUAL", nil] => "MUTUAL",
    ["AMEX", "NYSE ARCA"] => "NYSE-ARCA",
    ["INDEX", nil] => "INDEX",
    ["AMEX", "NYSE"] => "NYSE",
    ["INDEX", "Stock Indices"] => "INDEX",
    ["FINDEX", "Foreign Stock Indices"] => "INDEX",
    ["FINDEX", nil] => "INDEX",
    ["OTC", "NYSE"] => "NYSE",
    ["NYSE", "AMEX"] => "NYSE-MKT",
    ["NYSE", "OTC Markets Pink Sheets"] => "OTC-PINK",
    # ["TSX", "Toronto Stock Exchange"] => "???",
    # ["VSE", "TSX Venture Exchange"] => "???",
    # ["MSE", "Montreal Stock Exchange"] => "???",
    ["OTC", "AMEX"] => "NYSE-MKT",
    ["AMEX", "OTC Markets QX"] => "OTC-QX",
    ["NYSE", "NYSE ARCA"] => "NYSE-ARCA",
    ["NYSE", "BATS Global Markets"] => "BATS-CATCHALL",
    # ["LSE", "London Stock Exchange"] => "???",
    # ["ALBRTA", "Alberta Stock Exchange"] => "???",
    # ["OTC", "Grey Market"] => "",
    ["NYSE", "Nasdaq Capital Market"] => "NASDAQ-CM",
    ["AMEX", "OTC Markets QB"] => "OTC-QB",
    ["NYSE", "OTC Markets QX"] => "OTC-QX",
    # ["VSE", "Toronto Stock Exchange"] => "",
    ["OTC", "NYSE ARCA"] => "NYSE-ARCA",
    ["AMEX", "OTC Markets Pink Sheets"] => "OTC-PINK",
    ["NYSE", "OTC Markets QB"] => "OTC-QB",
    ["NYSE", "Nasdaq Global Market"] => "NASDAQ-GM",
    ["AMEX", "BATS Global Markets"] => "BATS-CATCHALL",
    ["OTC", "BATS Global Markets"] => "BATS-CATCHALL",
    # ["TSX", nil] => "",
    ["NYSE", "Nasdaq Global Select"] => "NASDAQ-GSM"
    # ["LSE", nil] => ""
  }

  attr_accessor :csi_client

  EquitySecurityType = "Equity"
  ETFSecurityType = "Exchange Traded Fund"
  ETNSecurityType = "Exchange Traded Note"
  MutualFundSecurityType = "Mutual Fund"
  IndexSecurityType = "Index"

  def initialize
    self.csi_client = CsiData::Client.new
    @exchange_memo = {}
  end

  def log(msg)
    Application.logger.info("#{Time.now} - #{msg}")
  end

  def import
    import_amex
    import_nyse
    import_nasdaq
    import_etfs
    import_etns
    import_mutual_funds
    import_us_stock_indices

    SecurityNameDatabaseRegistry.save_all
  end

  def import_amex
    log "Importing CSI Data symbols for AMEX."
    csi_securities = csi_client.amex
    import_securities(csi_securities, EquitySecurityType, today)
  end

  def import_nyse
    log "Importing CSI Data symbols for NYSE."
    csi_securities = csi_client.nyse
    import_securities(csi_securities, EquitySecurityType, today)
  end

  def import_nasdaq
    log "Importing CSI Data symbols for Nasdaq + OTC."
    csi_securities = csi_client.nasdaq_otc
    import_securities(csi_securities, EquitySecurityType, today)
  end

  def import_etfs
    log "Importing CSI Data symbols for ETFs."
    csi_securities = csi_client.etfs
    import_securities(csi_securities, ETFSecurityType, today)
  end

  def import_etns
    log "Importing CSI Data symbols for ETNs."
    csi_securities = csi_client.etns
    import_securities(csi_securities, ETNSecurityType, today)
  end

  def import_mutual_funds
    log "Importing CSI Data symbols for Mutual Funds."
    csi_securities = csi_client.mutual_funds
    import_securities(csi_securities, MutualFundSecurityType, today)
  end

  def import_us_stock_indices
    log "Importing CSI Data symbols for US Stock Indices."
    csi_securities = csi_client.us_stock_indices
    import_securities(csi_securities, IndexSecurityType, today)
  end

  def import_securities(csi_securities, default_security_type, active_date)
    log("Importing #{csi_securities.count} securities from CSI.")
    csi_securities.each do |csi_security|
      import_security(csi_security, default_security_type, active_date)
    end
  end

  def lookup_exchange(csi_security)
    csi_exchange_pair = [csi_security.exchange, csi_security.child_exchange]
    exchange_label = CSI_EXCHANGE_PAIR_TO_EXCHANGE_LABEL_MAP[ csi_exchange_pair ]
    @exchange_memo[exchange_label] ||= Exchange.first(label: exchange_label)
  end

  def today
    Date.today.strftime("%Y%m%d").to_i
  end

  # active_date is a yyyymmdd integer representation of a date within the active trading window of the security listing on the given exchange
  def import_security(csi_security, default_security_type, active_date)
    # 1. lookup exchange
    exchange = lookup_exchange(csi_security)

    if exchange
      # 2. lookup security in <exchange> by given symbol and active date
      listed_securities = ListedSecurity.
                     where(
                       exchange_id: exchange.id,
                       symbol: csi_security.symbol
                     ).
                     where {
                       (listing_start_date <= active_date) &
                       ((listing_end_date >= active_date) | (listing_end_date =~ nil))
                     }.to_a
      # securities = Security.
      #                association_join(:listed_securities).
      #                where(
      #                  listed_securities__exchange_id: exchange.id,
      #                  listed_securities__symbol: csi_security.symbol
      #                ).
      #                where {
      #                  (listing_start_date <= active_date) &
      #                  (listing_end_date >= active_date)
      #                }.to_a

      case listed_securities.count
      when 0                                  # if no listed securities found, find or create for the underlying security and then create the listed security
        log("Creating #{csi_security.symbol} in #{exchange.label}")
        create_listed_security(csi_security, exchange, default_security_type)
      when 1                                  # if one listed security found, update it
        listed_security = listed_securities.first
        log("Updating #{listed_security.symbol} (id=#{listed_security.id})")
        update_listed_security(listed_security, csi_security, default_security_type)
      else                                    # if multiple listed securities found, we have a data problem
        log("Error: There are multiple listed securities with symbol \"#{csi_security.symbol}\"")
        # composite_security = securities.detect {|security| security.exchange == composite_exchange }
        # if composite_security                 #   if security is in composite exchange, update the composite security
        #   log("Updating composite security #{composite_security.symbol} (id=#{composite_security.id})")
        #   update_security(composite_security, csi_security)
        # else                                  #   otherwise, security is in constituent exchange, so identify the proper security
        #   # we want to identify the security residing in the component exchange that is most preferred in the list of <expected_exchanges>, then update that security
        #   constituent_exchange_to_rank = expected_constituent_exchanges.each_with_index.to_h
        #   get_exchange_rank = ->(security) { constituent_exchange_to_rank[security.exchange] || 1_000_000_000 }
        #   security_with_most_preferred_constituent_exchange = securities.min_by {|security| get_exchange_rank.(security) }
        #
        #   log("Updating component security #{security_with_most_preferred_constituent_exchange.symbol} (id=#{security_with_most_preferred_constituent_exchange.id}); #{security_with_most_preferred_constituent_exchange.inspect}")
        #   update_security(security_with_most_preferred_constituent_exchange, csi_security)
        # end
      end
    end
  end


  # CsiData::Security is defined as
  # Struct.new(
  #   :csi_number,
  #   :symbol,
  #   :name,
  #   :exchange,
  #   :is_active,
  #   :start_date,
  #   :end_date,
  #   :sector,
  #   :industry,
  #   :conversion_factor,
  #   :switch_cf_date,
  #   :pre_switch_cf,
  #   :last_volume,
  #   :type,
  #   :child_exchange,
  #   :currency
  # )
  def create_listed_security(csi_security, exchange, default_security_type)
    security = find_security(csi_security.name, lookup_security_type(csi_security, default_security_type)) || create_security(csi_security, default_security_type)

    ListedSecurity.create(
      exchange: exchange,
      security: security,
      symbol: csi_security.symbol,
      listing_start_date: convert_date(csi_security.start_date),
      listing_end_date: convert_date(csi_security.end_date),
      csi_number: csi_security.csi_number.to_i
    )
  end

  def find_security(name, security_type_name)
    # security_names = Security.association_join(:security_type).where(security_type__name: security_type_name).select_map(:securities__name)
    db = SecurityNameDatabaseRegistry.get(security_type_name)
    search_key = extract_search_key_from_security_name(name)
    matches = db.ranked_search(search_key, APPROXIMATE_SEARCH_THRESHOLD)

    # search for securities by approximate matching against search_key
    securities = case matches.count
    when 0
      Security.association_join(:security_type).where(security_type__name: security_type_name, securities__name: name).to_a
    when 1
      Security.association_join(:security_type).where({security_type__name: security_type_name}, Sequel.or(securities__name: name, search_key: matches.first.value)).to_a
    else
      matching_names = matches.map {|match| "#{match.value} - #{match.score}" }
      best_match_search_key = matches.first.value
      search_key_best_match_score = matches.first.score
      if search_key_best_match_score == 1 ||
          is_match_correct?("Is \"#{best_match_search_key}\" a match for the search phrase \"#{search_key}\"? Score=#{matches.first.score} (Y/n) ")
        log("Warning: Searching for ambiguous security name. Search key #{search_key} (name=#{name}   security_type_name=#{security_type_name}) is being mapped to #{matching_names.first}. The #{matches.count} matches were:\n#{matching_names.join("\n")}")
        Security.association_join(:security_type).where({security_type__name: security_type_name}, Sequel.or(securities__name: name, search_key: best_match_search_key)).to_a
      else
        log("Warning: Searching for ambiguous security name. Search key #{search_key} (name=#{name}   security_type_name=#{security_type_name}) is NOT being mapped to #{matching_names.first}. The #{matches.count} matches were:\n#{matching_names.join("\n")}")
        []
      end
    end

    # figure out which of the matched securities is the closest name match
    case securities.count
    when 0
      nil
    when 1
      securities.first
    else
      db = SecurityNameDatabase.new
      security_name_to_security = securities.reduce({}) {|memo, security| memo[security.name.downcase] = security; memo }
      securities.each {|security| db.add(security.name.downcase) }
      matches = db.ranked_search(name, APPROXIMATE_SEARCH_THRESHOLD)
      closest_matching_name = matches.first.value
      closest_matching_security = security_name_to_security[closest_matching_name]
      log("Warning: Multiple securities found for search key #{search_key} (name=#{name}   security_type_name=#{security_type_name}). Closest match to \"#{name}\" is security id=#{closest_matching_security.id} name=#{closest_matching_security.name}")
      closest_matching_security
    end
  end

  def is_match_correct?(prompt = "Is match correct? (Y/n): ")
    prompt_yes_no(prompt)
  end

  def prompt_yes_no(prompt = "(Y/n): ")
    print prompt
    yes_or_no = case STDIN.getch.strip.downcase
    when "y", ""
      true
    else
      false
    end
  end

  def create_security(csi_security, default_security_type)
    security_type_name = lookup_security_type(csi_security, default_security_type)
    security_type = find_or_create_security_type(security_type_name)
    industry = find_or_create_industry(csi_security.industry)
    sector = find_or_create_sector(csi_security.sector)
    security = Security.create(
      security_type: security_type,
      industry: industry,
      sector: sector,
      name: csi_security.name,
      search_key: extract_search_key_from_security_name(csi_security.name)
    )

    db = SecurityNameDatabaseRegistry.get(security_type_name)
    search_key = extract_search_key_from_security_name(csi_security.name)
    db.add(search_key)

    security
  end

  # CsiData::Security is defined as
  # Struct.new(
  #   :csi_number,
  #   :symbol,
  #   :name,
  #   :exchange,
  #   :is_active,
  #   :start_date,
  #   :end_date,
  #   :sector,
  #   :industry,
  #   :conversion_factor,
  #   :switch_cf_date,
  #   :pre_switch_cf,
  #   :last_volume,
  #   :type,
  #   :child_exchange,
  #   :currency
  # )
  def update_listed_security(listed_security, csi_security, default_security_type)
    security = listed_security.security

    # update Security
    replacement_attributes = {}

    security_type = find_or_create_security_type(lookup_security_type(csi_security, default_security_type))
    replacement_attributes[:security_type_id] = security_type.id if security.security_type_id != security_type.id

    industry = find_or_create_industry(csi_security.industry || UNKNOWN_INDUSTRY_NAME)
    replacement_attributes[:industry_id] = industry.id if security.industry_id != industry.id

    sector = find_or_create_sector(csi_security.sector || UNKNOWN_SECTOR_NAME)
    replacement_attributes[:sector_id] = sector.id if security.sector_id != sector.id

    replacement_attributes[:name] = csi_security.name if security.name != csi_security.name
    replacement_attributes[:search_key] = extract_search_key_from_security_name(csi_security.name) if security.name != csi_security.name

    if !replacement_attributes.empty?
      log("Updating security #{security.inspect} with attributes: #{replacement_attributes.inspect}")
      security.update(replacement_attributes)
    end


    # update ListedSecurity
    replacement_attributes = {}
    replacement_attributes[:symbol] = csi_security.symbol if listed_security.symbol != csi_security.symbol
    replacement_attributes[:listing_start_date] = convert_date(csi_security.start_date) if listed_security.listing_start_date != convert_date(csi_security.start_date)
    replacement_attributes[:listing_end_date] = convert_date(csi_security.end_date) if listed_security.listing_end_date != convert_date(csi_security.end_date)
    replacement_attributes[:csi_number] = csi_security.csi_number if listed_security.csi_number != csi_security.csi_number

    if !replacement_attributes.empty?
      log("Updating listed security #{listed_security.inspect} with attributes: #{replacement_attributes.inspect}")
      listed_security.update(replacement_attributes)
    end

    listed_security
  rescue Sequel::ValidationFailed, Sequel::HookFailed => e
    log "Can't import #{csi_security.inspect}: #{e.message}"
  rescue => e
    log "Can't import #{csi_security.inspect}: #{e.message}"
    log e.backtrace.join("\n")
  end


  # csi_date is a string of the form "1993-02-01"
  # returns the integer yyyymmdd representation of the csi_date
  def convert_date(csi_date)
    if csi_date
      csi_date.gsub("-","").to_i unless csi_date.empty?
    end
  end

  def lookup_security_type(csi_security, default_security_type)
    if csi_security.type.nil? || csi_security.type == UNKNOWN_SECURITY_TYPE
      default_security_type
    else
      csi_security.type
    end
  end

  def extract_search_key_from_security_name(security_name)
    security_name.downcase
  end

  def find_or_create_security_type(security_type_name)
    if security_type_name && !security_type_name.empty?
      SecurityType.first(name: security_type_name) || SecurityType.create(name: security_type_name)
    end
  end

  def find_or_create_sector(sector_name)
    if sector_name && !sector_name.empty?
      Sector.first(name: sector_name) || Sector.create(name: sector_name)
    end
  end

  def find_or_create_industry(industry_name)
    if industry_name && !industry_name.empty?
      Industry.first(name: industry_name) || Industry.create(name: industry_name)
    end
  end

end
