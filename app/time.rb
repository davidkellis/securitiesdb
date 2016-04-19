require 'date'

require_relative 'date'

LocalTime = Struct.new(:hour, :minute, :second)

class DateTime
  class << self
    def timestamp_to_dt(timestamp)
      ts_str = timestamp.to_s
      year = ts_str.slice(0, 4).to_i
      month = ts_str.slice(4, 2).to_i
      day = ts_str.slice(6, 2).to_i
      hour = ts_str.slice(8, 2).to_i
      min = ts_str.slice(10, 2).to_i
      sec = ts_str.slice(12, 2).to_i
      DateTime.new(year, month, day, hour, min, sec)
    end

    def to_timestamp(t)
      ((((t.year * 100 + t.month) * 100 + t.day) * 100 + t.hour) * 100 + t.minute) * 100 + t.second
    end
  end
end

module DateTimeExtensions
  refine DateTime do
    def to_timestamp
      DateTime.to_timestamp(self)
    end
  end
end
