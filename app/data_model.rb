require 'sequel'
require 'bigdecimal'

Sequel::Model.raise_on_save_failure = false

POSTGRES_BIGINT_MAX = 2**63-1

class Exchange < Sequel::Model
  one_to_many :listed_securities
  many_to_many :securities, join_table: :listed_securities

  many_to_one :composite_exchange, class: self
  one_to_many :constituent_exchanges, key: :composite_exchange_id, class: self

  # CBOE - Chicago Board Options Exchange
  def self.cboe
    @cboe ||= where(label: "CBOE")
  end

  # see http://www.nasdaqomx.com/transactions/markets/nasdaq
  # The NASDAQ Stock Market is comprised of three market tiers:
  # - The NASDAQ Global Select Market (bloomberg: UW) - most exclusive Nasdaq market
  # - The NASDAQ Global Market (formerly the NASDAQ National Market) (bloomberg: UQ) - more exclusive than Capital Market, less exclusive than Global Select Market
  # - The NASDAQ Capital Market (formerly the NASDAQ SmallCap Market) (bloomberg: UR) - least exclusive Nasdaq Market
  def self.nasdaq
    @nasdaq ||= where(label: ["NASDAQ-CM", "NASDAQ-GM", "NASDAQ-GSM", "NASDAQ-CATCHALL"])
  end

  def self.nyse
    @nyse ||= where(label: ["NYSE", "NYSE-CATCHALL"])          # NYSE
  end

  def self.nyse_arca
    @nyse_arca ||= where(label: "NYSE-ARCA")     # NYSE Arca
  end

  def self.nyse_mkt
    @amex ||= where(label: "NYSE-MKT")      # NYSE MKT - formerly the American Stock Exchange (AMEX)
  end

  def self.otc_bulletin_board
    @otc_bb ||= where(label: "OTCBB")
  end

  def self.otc_markets
    @otc_markets ||= where(label: ["OTC-QX", "OTC-QB", "OTC-CATCHALL"])
  end

  def self.pink_sheets
    @pink_sheets ||= where(label: "OTC-PINK")
  end

  def self.us_stock_exchanges
    nyse_mkt.
      union(nasdaq).
      union(nyse).
      union(nyse_arca).
      union(otc_markets).
      union(otc_bulletin_board)
  end

  def self.us_exchanges
    us_stock_exchanges.to_a +
      cboe.to_a
  end

  def self.indices
    @indices ||= where(label: "INDEX").to_a
  end

  # def self.catch_all_mutual
  #   @catch_all_mutual ||= first(label: "MUTUAL", is_composite_exchange: false)
  # end
  #
  # def self.catch_all_stock
  #   @catch_all_stock ||= first(label: "STOCK", is_composite_exchange: false)
  # end
  #
  # def self.catch_all_etp
  #   @catch_all_stock ||= first(label: "ETP", is_composite_exchange: false)
  # end
  #
  # def self.catch_all_option
  #   @catch_all_stock ||= first(label: "OPTION", is_composite_exchange: false)
  # end
  #
  # def self.catch_all_future
  #   @catch_all_stock ||= first(label: "FUTURE", is_composite_exchange: false)
  # end
end

class Classification < Sequel::Model
  one_to_many :classification_stats_summaries
  one_to_many :security_classifications
  many_to_many :securities, :join_table => :security_classifications

  class << self
    def cache
      @cache ||= LruCache.new(50)
    end
  end

  def self.lookup(major, minor, micro)
    cache.get_or_set("#{major}/#{minor}/#{micro}") do
      Classification.where(major: major, minor: minor, micro: micro).first
    end
  end

  def self.lookup_or_create(major, minor, micro)
    lookup(major, minor, micro) || begin
      classification = Classification.create(major: major, minor: minor, micro: micro)
      cache.set("#{major}/#{minor}/#{micro}", classification)
      classification
    end
  end

  def name
    "#{major}__#{minor}__#{micro}"
  end
end

# join model between Classification and Security
class SecurityClassification < Sequel::Model
  extend Forwardable

  many_to_one :classification
  many_to_one :security

  def_delegators :classification, :major, :minor, :micro

  def name
    "#{date}__#{classification.name}"
  end
end

class SecurityVariable < Sequel::Model
  one_to_many :classification_summaries
end

# stores statsistics that describe the distribution of a variable taken over all the securities within a given classification
class ClassificationSummary < Sequel::Model
  many_to_one :classification
  many_to_one :security_variable
end

class SecurityType < Sequel::Model
  one_to_many :securities

  def self.stock
    first(name: "Common Stock")   # Market Sector/Security Type => Equity/Common Stock
  end

  def self.etp
    first(name: "ETP")            # Market Sector/Security Type => Equity/ETP
  end

  def self.funds
    where(name: ["Fund of Funds", "Mutual Fund", "Open-End Fund"]).to_a    # Equity/{"Fund of Funds", "Mutual Fund", "Open-End Fund"}
  end
end

class ListedSecurity < Sequel::Model
  many_to_one :exchange
  many_to_one :security
end

class Security < Sequel::Model
  one_to_many :listed_securities
  many_to_many :exchanges, join_table: :listed_securities

  one_to_many :eod_bars
  one_to_many :corporate_actions
  one_to_many :fundamental_datasets
  one_to_many :options, key: :underlying_security_id    # this is the relation "security is the underlying security for many options; option belongs to one underlying security"

  many_to_one :security_type
  one_to_many :security_classifications, eager: :classification
  many_to_many :classifications, :join_table => :security_classifications

  # This is the relation "security identifies one options contract; options contract is identified by one security;
  # Alternatively, the security and the option both uniquely identify a single options contract,
  # specified by the 4-tuple (underlying security, expiration, strike, callOrPut)"
  one_to_one :option

  # CBOE - Chicago Board Options Exchange
  def self.cboe
    qualify.
      join(:listed_securities, :security_id => :id).
      join(:exchanges, :id => :listed_securities__exchange_id).
      where(:exchanges__label => "CBOE")
  end

  def self.nyse_mkt
    qualify.
      join(:listed_securities, :security_id => :id).
      join(:exchanges, :id => :listed_securities__exchange_id).
      where(:exchanges__label => "NYSE-MKT")
  end

  def self.nasdaq
    # see http://www.nasdaqomx.com/transactions/markets/nasdaq
    # The NASDAQ Stock Market is comprised of three market tiers:
    # - The NASDAQ Global Select Market (bloomberg: UW)
    # - The NASDAQ Global Market (formerly the NASDAQ National Market) (bloomberg: UQ)
    # - The NASDAQ Capital Market (formerly the NASDAQ SmallCap Market) (bloomberg: UR)
    qualify.
      join(:listed_securities, :security_id => :id).
      join(:exchanges, :id => :listed_securities__exchange_id).
      where(:exchanges__label => ["NASDAQ-CM", "NASDAQ-GM", "NASDAQ-GSM"])
  end

  def self.nyse
    qualify.
      join(:listed_securities, :security_id => :id).
      join(:exchanges, :id => :listed_securities__exchange_id).
      where(:exchanges__label => "NYSE")
  end

  def self.us_stock_exchanges
    qualify.
      join(:listed_securities, :security_id => :id).
      join(:exchanges, :id => :listed_securities__exchange_id).
      where(:exchanges__label => Exchange.us_stock_exchanges.map(&:label))
  end

  def self.us_exchanges
    qualify.
      join(:listed_securities, :security_id => :id).
      join(:exchanges, :id => :listed_securities__exchange_id).
      where(:exchanges__label => Exchange.us_exchanges.map(&:label))
  end

  def self.indices
    qualify.
      join(:listed_securities, :security_id => :id).
      join(:exchanges, :id => :listed_securities__exchange_id).
      where(:exchanges__label => "INDEX")
  end


  def classify(major, minor, micro, datestamp = Date.today_datestamp)
    classification = Classification.lookup_or_create(major, minor, micro)
    existing_security_classification = security_classifications_dataset.first(date: datestamp, classification: classification)
    existing_security_classification ||= self.add_security_classification(date: datestamp, classification: classification)
  end

  def unclassify(major, minor, micro, datestamp = nil)
    classification = Classification.lookup_or_create(major, minor, micro)
    if datestamp
      existing_security_classification = security_classifications_dataset.first(date: datestamp, classification: classification)
      self.remove_security_classification(existing_security_classification) if existing_security_classification
    else
      security_classifications_dataset.where(classification: classification).delete
    end
  end
end

class Option < Sequel::Model
  many_to_one :security
  many_to_one :underlying_security, class: Security

  one_to_many :eod_option_quotes
end

class EodOptionQuote < Sequel::Model
  many_to_one :option
end

class DataVendor < Sequel::Model
  one_to_many :time_series
end

class UpdateFrequency < Sequel::Model
  DAILY = "Daily"
  WEEKLY = "Weekly"
  MONTHLY = "Monthly"
  QUARTERLY = "Quarterly"
  YEARLY = "Yearly"
  IRREGULAR = "Irregular"

  one_to_many :time_series

  def self.lookup(label)
    @label_to_update_frequency ||= all.to_a.reduce({}) {|memo, update_frequency| memo[update_frequency.label] = update_frequency; memo }
    @label_to_update_frequency[label]
  end

  def self.daily
    lookup(DAILY)
  end

  def self.weekly
    lookup(WEEKLY)
  end

  def self.monthly
    lookup(MONTHLY)
  end

  def self.quarterly
    lookup(QUARTERLY)
  end

  def self.yearly
    lookup(YEARLY)
  end

  def self.irregular
    lookup(IRREGULAR)
  end
end

class TimeSeries < Sequel::Model
  many_to_one :data_vendor
  many_to_one :update_frequency

  one_to_many :daily_observations
  one_to_many :weekly_observations
  one_to_many :monthly_observations
  one_to_many :quarterly_observations
  one_to_many :yearly_observations
  one_to_many :irregular_observations

  one_to_one :fundamental_dataset
end

class EodBar < Sequel::Model
  many_to_one :security

  def adjusted_close(frame_of_reference_datestamp)
    corporate_action_tsm = CorporateActionLoader.get(self.security)
    cumulative_adjustment_factor = CorporateActionAdjustment.calculate_cumulative_adjustment_factor(self.security, self.date, frame_of_reference_datestamp)
    CorporateActionAdjustment.adjust_price(self.close.to_f, cumulative_adjustment_factor)
  end
end

class CorporateActionType < Sequel::Model
  one_to_many :corporate_actions

  def self.cash_dividend
    @cash_dividend ||= first(name: "Cash Dividend")
  end

  def self.split
    @split ||= first(name: "Split")
  end
end

class CorporateAction < Sequel::Model
  many_to_one :security
  many_to_one :corporate_action_type

  def self.create_cash_dividend(security_id, ex_date, declaration_date, record_date, payable_date, adjustment_factor, dividend_amount)
    CorporateAction.create(
      security_id: security_id,
      corporate_action_type_id: CorporateActionType.cash_dividend.id,
      ex_date: ex_date,
      declaration_date: declaration_date,
      record_date: record_date,
      payable_date: payable_date,
      adjustment_factor: adjustment_factor,
      value: dividend_amount
    )
  end

  def self.create_split(security_id, ex_date, adjustment_factor)
    CorporateAction.create(
      security_id: security_id,
      corporate_action_type_id: CorporateActionType.split.id,
      ex_date: ex_date,
      adjustment_factor: adjustment_factor
    )
  end
end

CorporateAction.dataset_module do
  def cash_dividends
    qualify.
      join(:corporate_action_types, :id => :corporate_action_type_id).
      where(Sequel.qualify(:corporate_action_types, :name) => CorporateActionType.cash_dividend.name)
  end

  def splits
    qualify.
      join(:security_types, :id => :security_type_id).
      where(Sequel.qualify(:security_types, :name) => CorporateActionType.split.name)
  end
end

class FundamentalAttribute < Sequel::Model
  one_to_many :fundamental_datasets
end

class FundamentalDimension < Sequel::Model
  INSTANTANEOUS = "INST"
  ARQ = "ARQ"
  ARY = "ARY"
  ART_Q = "ART-Q"
  MRQ = "MRQ"
  MRY = "MRY"
  MRT_Q = "MRT-Q"

  one_to_many :fundamental_datasets

  def self.lookup(name)
    @name_to_dimension ||= all.to_a.reduce({}) {|memo, dimension| memo[dimension.name] = dimension ; memo }
    @name_to_dimension[name]
  end

  def self.instantaneous
    lookup(INSTANTANEOUS)
  end

  # as reported, quarterly
  def self.arq
    lookup(ARQ)
  end

  # as reported, annually
  def self.ary
    lookup(ARY)
  end

  # as reported, TTM; aggregated over quarterly observations
  def self.art_q
    lookup(ART_Q)
  end

  # most recent reported, quarterly
  def self.mrq
    lookup(MRQ)
  end

  # most recent reported, annually
  def self.mry
    lookup(MRY)
  end

  # most recent reported, TTM; aggregated over quarterly observations
  def self.mrt_q
    lookup(MRT_Q)
  end
end

class FundamentalDataset < Sequel::Model
  many_to_one :security
  many_to_one :fundamental_attribute
  many_to_one :fundamental_dimension
  many_to_one :time_series      # fundamental_datasets-to-time_series is really a one-to-one association;
                                # the sequel library requires that the relation on the table with the foreign key be
                                # defined as many_to_one (see http://sequel.jeremyevans.net/rdoc/files/doc/association_basics_rdoc.html#label-Differences+Between+many_to_one+and+one_to_one)

  def summary
    {
      exchange_name: security.exchange.name,
      security_name: security.name,
      security_symbol: security.symbol,
      fundamental_attribute_label: fundamental_attribute.label,
      fundamental_attribute_name: fundamental_attribute.name,
      fundamental_dimension_name: fundamental_dimension.name
    }
  end
end

# time dimension-specific time series classes (e.g. daily, monthly, yearly)
class DailyObservation < Sequel::Model
  many_to_one :time_series
end

class WeeklyObservation < Sequel::Model
  many_to_one :time_series
end

class MonthlyObservation < Sequel::Model
  many_to_one :time_series
end

class QuarterlyObservation < Sequel::Model
  many_to_one :time_series
end

class YearlyObservation < Sequel::Model
  many_to_one :time_series
end

class IrregularObservation < Sequel::Model
  many_to_one :time_series
end
