class FindEodBar
  class << self
    def at_or_earlier_than(security, datestamp)
      # first attempt to find the eod_bar - search this year's worth of eod bars
      tsmap = EodBarLoader.get(security, datestamp)
      eod_bar = tsmap.latest_value_at_or_earlier_than(datestamp)
      return eod_bar if eod_bar

      # second attempt to find the eod_bar - search previous year's worth of eod bars
      year, month, day = *Date.datestamp_components(datestamp)
      tsmap = EodBarLoader.get(security, Date.build_datestamp(year - 1, month, day))
      eod_bar = tsmap.latest_value_at_or_earlier_than(datestamp)
      return eod_bar if eod_bar

      # third attempt to find the eod bar - query DB for latest eod bar observed on or earlier than <datestamp>
      security.eod_bars_dataset.
        where { date <= datestamp }.
        order(Sequel.desc(:date)).
        limit(1).
        first
    end
  end
end
