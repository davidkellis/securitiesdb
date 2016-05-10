module CorporateActionAdjustment
  class << self
    def adjust_price(unadjusted_price, cumulative_adjustment_factor)
      unadjusted_price.to_f / cumulative_adjustment_factor
    end

    def adjust_divident_payout(unadjusted_dividend_payout, cumulative_adjustment_factor)
      unadjusted_dividend_payout.to_f / cumulative_adjustment_factor
    end

    def adjust_share_count(unadjusted_share_count, cumulative_adjustment_factor)
      unadjusted_share_count * cumulative_adjustment_factor
    end

    def adjust_volume(unadjusted_volume, cumulative_adjustment_factor)
      unadjusted_volume * cumulative_adjustment_factor
    end


    # observation_date is the date that a price, volume, divident payout, or share count observation was observed
    # The observation made on the observation_date is the value we are wanting to adjust for splits/dividends.
    # adjustment_base_date is the date (occurring after observation_date) from which our adjustment "look back" is made
    #
    # In other words, this function tells us the cumulative adjustment factor to apply to historical observations made at the obsesrvation_date
    # assuming we are looking back at those observations from the given base date (in the relative future, compared to the historical observation)
    #
    # adjustment_base_date is our frame of reference for looking back in history, viewing historical observations from the moment in time
    # specified by adjustment_base_date
    #
    # NOTE: observation_date < adjustment_base_date
    def calculate_cumulative_adjustment_factor(security, observation_datestamp, adjustment_base_datestamp)
      # 1. look up all corporate actions between (observation_date, adjustment_base_date]
      corporate_actions = FindCorporateAction.between(security, observation_datestamp, adjustment_base_datestamp, false, true)

      # 2. multiply the adjustment factors for each corporate action together
      corporate_actions.reduce(1.0) {|product, corporate_action| product * corporate_action.adjustment_factor }
    end
  end
end
