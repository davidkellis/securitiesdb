class FindCorporateAction
  class << self
    def between(security, start_datestamp, end_datestamp, inclusive_of_start_datestamp = true, inclusive_of_end_datestamp = false)
      corporate_action_time_series_map = CorporateActionLoader.get(security)
      corporate_action_time_series_map.between(start_datestamp, end_datestamp, inclusive_of_start_datestamp, inclusive_of_end_datestamp).values
    end
  end
end
