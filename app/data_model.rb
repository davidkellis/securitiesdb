require 'sequel'
require 'bigdecimal'

Sequel::Model.raise_on_save_failure = false

POSTGRES_BIGINT_MAX = 2**63-1

class Exchange < Sequel::Model
  one_to_many :securities

  many_to_one :composite_exchange, class: self
  one_to_many :constituent_exchanges, key: :composite_exchange_id, class: self

  # NYSE MKT - formerly the American Stock Exchange (AMEX)
  def self.amex
    @amex ||= where(label: "UA")
  end

  # CBOE - Chicago Board Options Exchange
  def self.cboe
    @cboe ||= where(label: "CBOE")
  end

  def self.dow_jones
    @dow ||= where(label: "DJI")
  end

  # see http://www.nasdaqomx.com/transactions/markets/nasdaq
  # The NASDAQ Stock Market is comprised of three market tiers:
  # - The NASDAQ Global Select Market (bloomberg: UW) - most exclusive Nasdaq market
  # - The NASDAQ Global Market (formerly the NASDAQ National Market) (bloomberg: UQ) - more exclusive than Capital Market, less exclusive than Global Select Market
  # - The NASDAQ Capital Market (formerly the NASDAQ SmallCap Market) (bloomberg: UR) - least exclusive Nasdaq Market
  def self.nasdaq
    @nasdaq ||= where(label: ["UW", "UQ", "UR"])
  end

  def self.nyse
    @nyse ||= where(label: "UN")      # NYSE
  end

  def self.nyse_arca
    @nyse ||= where(label: "UP")      # NYSE Arca
  end

  def self.otc_bulletin_board
    @otc_bb ||= where(label: "UU")
  end

  def self.otc
    @otc ||= where(label: "UV")
  end

  def self.otc_markets
    @otc_markets ||= where(label: "PQ")
  end

  def self.russell
    @russell ||= where(label: ["RUSS", "RUSL"])
  end

  def self.us_stock_exchanges
    amex.union(nasdaq).union(nyse)
  end

  def self.us_exchanges
    us_composite.constituent_exchanges +    # constituent_exchanges = ["PQ", "UA", "UB", "UC", "UD", "UE", "UF", "UL", "UM", "UN", "UO", "UP", "UQ", "UR", "UT", "UU", "UV", "UW", "UX", "VJ", "VK", "VY"]
      cboe.to_a +
      dow_jones.to_a +
      russell.to_a +
      [us_composite]
  end

  def self.us_composite
    @us_composite ||= first(label: "US", is_composite_exchange: true)
  end

  def self.catch_all_index
    @catch_all_index ||= first(label: "INDEX", is_composite_exchange: false)
  end

  def self.catch_all_mutual
    @catch_all_mutual ||= first(label: "MUTUAL", is_composite_exchange: false)
  end

  def self.catch_all_stock
    @catch_all_stock ||= first(label: "STOCK", is_composite_exchange: false)
  end

  def self.catch_all_etp
    @catch_all_stock ||= first(label: "ETP", is_composite_exchange: false)
  end
end

class Industry < Sequel::Model
  one_to_many :securities
end

class Sector < Sequel::Model
  one_to_many :securities
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

class Security < Sequel::Model
  one_to_many :eod_bars
  one_to_many :corporate_actions
  one_to_many :fundamental_datasets
  one_to_many :options, key: :underlying_security_id    # this is the relation "security is the underlying security for many options; option belongs to one underlying security"

  many_to_one :exchange
  many_to_one :security_type
  many_to_one :industry
  many_to_one :sector

  # This is the relation "security identifies one options contract; options contract is identified by one security;
  # Alternatively, the security and the option both uniquely identify a single options contract,
  # specified by the 4-tuple (underlying security, expiration, strike, callOrPut)"
  one_to_one :option

  # CBOE - Chicago Board Options Exchange
  def self.cboe
    qualify.
      join(:exchanges, :id => :exchange_id).
      where(Sequel.qualify(:exchanges, :label) => "CBOE")
  end

  def self.amex
    qualify.
      join(:exchanges, :id => :exchange_id).
      where(Sequel.qualify(:exchanges, :label) => "UA")
  end

  def self.nasdaq
    # see http://www.nasdaqomx.com/transactions/markets/nasdaq
    # The NASDAQ Stock Market is comprised of three market tiers:
    # - The NASDAQ Global Select Market (bloomberg: UW)
    # - The NASDAQ Global Market (formerly the NASDAQ National Market) (bloomberg: UQ)
    # - The NASDAQ Capital Market (formerly the NASDAQ SmallCap Market) (bloomberg: UR)
    qualify.
      join(:exchanges, :id => :exchange_id).
      where(Sequel.qualify(:exchanges, :label) => ["UW", "UQ", "UR"])
  end

  def self.nyse
    qualify.
      join(:exchanges, :id => :exchange_id).
      where(Sequel.qualify(:exchanges, :label) => "UN")   # NYSE (not NYSE Arca)
  end

  def self.us_stock_exchanges
    amex.union(nasdaq).union(nyse)
  end

  def self.us_composite
    qualify.
      join(:exchanges, :id => :exchange_id).
      where(Sequel.qualify(:exchanges, :label) => "US")
  end

  def self.us_exchanges
    qualify.
      join(:exchanges, :id => :exchange_id).
      where(Sequel.qualify(:exchanges, :label) => Exchange.us_exchanges.map(&:label))
  end

  def self.indices
    qualify.
      join(:security_types, :id => :security_type_id).
      where(Sequel.qualify(:security_types, :market_sector) => "Index")
  end
end

class EodBar < Sequel::Model
  many_to_one :security
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

  def self.create_cash_dividend(security_id, ex_date, declaration_date, record_date, payable_date, adjustment_factor)
    CorporateAction.create(
      security_id: security_id,
      corporate_action_type_id: CorporateActionType.cash_dividend.id,
      ex_date: ex_date,
      declaration_date: declaration_date,
      record_date: record_date,
      payable_date: payable_date,
      adjustment_factor: adjustment_factor
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
  many_to_one :time_series      # fundamental_datasets-to-time_series is really a one-to-one association

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


class Option < Sequel::Model
  many_to_one :security
  many_to_one :underlying_security, class: Security

  one_to_many :eod_option_quotes
end

class EodOptionQuote < Sequel::Model
  many_to_one :option
end
