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

class QuarterlyReport < Sequel::Model
  many_to_one :security

  def deserialized_income_statement
    FinancialStatement.decode(income_statement)
  end

  def deserialized_balance_sheet
    FinancialStatement.decode(balance_sheet)
  end

  def deserialized_cash_flow_statement
    FinancialStatement.decode(cash_flow_statement)
  end
end

class AnnualReport < Sequel::Model
  many_to_one :security
end

class Strategy < Sequel::Model
  module Names
    BuyAndHold = "Buy And Hold"
  end

  one_to_many :trial_sets
end

class TrialSet < Sequel::Model
  many_to_one :strategy
  one_to_many :trials
  one_to_many :trial_set_distributions
  many_to_many :sampling_distributions, :join_table => :trial_set_distributions, :right_key => :id, :right_primary_key => :trial_set_distribution_id
  many_to_many :securities, :join_table => :securities_trial_sets
end

class TrialSetDistribution < Sequel::Model
  module Attributes
    Return = 'return'
    Mae = 'mae'
    Mfe = 'mfe'
  end

  many_to_one :trial_set
  many_to_one :trial_set_distribution_type
  one_to_many :sampling_distributions

  def self.weekly_non_overlapping_trials
    qualify.
      join(:trial_set_distribution_types, :id => :trial_set_distribution_type_id).
      where(
        Sequel.qualify(:trial_set_distribution_types, :name) => TrialSetDistributionType::Names::WeeklyNonOverlappingTrials
      ).
      first
  end
end

class TrialSetDistributionType < Sequel::Model
  module Names
    WeeklyNonOverlappingTrials = "WeeklyNonOverlappingTrials"
  end

  one_to_many :trial_set_distributions
end

class SampleStatistic < Sequel::Model
  one_to_many :sampling_distributions
end

class SamplingDistribution < Sequel::Model
  many_to_one :trial_set_distribution
  one_through_one :trial_set, :join_table => :trial_set_distributions, :left_key => :id, :left_primary_key => :trial_set_distribution_id, :right_key => :trial_set_id
  many_to_one :sample_statistic
end

class Trial < Sequel::Model
  many_to_one :trial_set

  def deserialized_transaction_log
    TransactionLog.decode(transaction_log)
  end

  def deserialized_portfolio_value_log
    PortfolioValueLog.decode(portfolio_value_log)
  end
  #
  # def securities
  #   trial_set.securities
  # end
  #
  # # returns a list of BigDecimals
  # def portfolio_values
  #   @portfolio_values ||= deserialized_portfolio_value_log.portfolioValues.map {|portfolio_value| BigDecimal.new(portfolio_value.value) }
  # end
  #
  # def portfolio_value_stats
  #   @online_variance ||= begin
  #     online_variance = Stats::OnlineVariance.new
  #     portfolio_values.each {|value| online_variance.push(value) }
  #     online_variance
  #   end
  # end
  #
  # def yield
  #   initial_value = portfolio_values.first
  #   final_value = portfolio_values.last
  #   final_value / initial_value
  # end
  #
  # def maximum_favorable_excursion
  #   initial_value = portfolio_values.first
  #   max_value = portfolio_value_stats.max
  #   max_value / initial_value
  # end
  #
  # def maximum_adverse_excursion
  #   initial_value = portfolio_values.first
  #   min_value = portfolio_value_stats.min
  #   min_value / initial_value
  # end
  #
  # def variance
  #   portfolio_value_stats.variance
  # end
end
