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
      russell.to_a
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
end

class Security < Sequel::Model
  one_to_many :eod_bars
  one_to_many :corporate_actions
  one_to_many :fundamental_data_points

  many_to_one :exchange
  many_to_one :security_type
  many_to_one :industry
  many_to_one :sector

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
    us_stock_exchanges.union(cboe)
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

class FundamentalAttributes < Sequel::Model
  one_to_many :fundamental_data_points
end

class FundamentalDataPoint < Sequel::Model
  many_to_one :security
  many_to_one :fundamental_attribute
end
