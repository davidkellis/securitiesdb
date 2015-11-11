require 'sequel'
require 'bigdecimal'

Sequel::Model.raise_on_save_failure = false

POSTGRES_BIGINT_MAX = 2**63-1

class Exchange < Sequel::Model
  one_to_many :securities

  # CBOE - Chicago Board Options Exchange
  def self.cboe
    @cboe ||= where(label: "CBOE")
  end

  # NYSE MKT - formerly the American Stock Exchange (AMEX)
  def self.amex
    @amex ||= where(label: "UA")
  end

  # see http://www.nasdaqomx.com/transactions/markets/nasdaq
  # The NASDAQ Stock Market is comprised of three market tiers:
  # - The NASDAQ Global Select Market (bloomberg: UW)
  # - The NASDAQ Global Market (formerly the NASDAQ National Market) (bloomberg: UQ)
  # - The NASDAQ Capital Market (formerly the NASDAQ SmallCap Market) (bloomberg: UR)
  def self.nasdaq
    @nasdaq ||= where(label: ["UW", "UQ", "UR"])
  end

  def self.nyse
    @nyse ||= where(label: "UN")      # NYSE (not NYSE Arca)
  end

  def self.otc_bulletin_board
    @otc ||= where(label: "UU")
  end

  def self.otc
    @non_otc ||= where(label: "UV")
  end

  def self.us_composite
    @us_composite ||= where(label: "US")
  end

  def self.us_stock_exchanges
    amex.union(nasdaq).union(nyse)
  end

  def self.us_exchanges
    us_stock_exchanges.union(cboe)
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
  one_to_many :annual_reports
  one_to_many :quarterly_reports

  many_to_one :exchange
  many_to_one :security_type
  many_to_one :industry
  many_to_one :sector

  many_to_many :trial_sets, :join_table => :securities_trial_sets

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
end

Security.dataset_module do
  def cash_dividends
    # todo: ???
  end

  def splits
    # todo: ???
  end
end

class EodBar < Sequel::Model
  many_to_one :security
end

class CorporateActionTypes < Sequel::Model
  one_to_many :corporate_actions
end

  # todo - finish this model
class CorporateAction < Sequel::Model
  many_to_one :security
  many_to_one :corporate_action_type
end
