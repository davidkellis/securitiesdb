require 'date'

class Date
  module DayOfWeek
    Monday = 1
    Tuesday = 2
    Wednesday = 3
    Thursday = 4
    Friday = 5
    Saturday = 6
    Sunday = 7
  end

  module Month
    January = 1
    February = 2
    March = 3
    April = 4
    May = 5
    June = 6
    July = 7
    August = 8
    September = 9
    October = 10
    November = 11
    December = 12
  end

  class << self
    def today_datestamp
      date_to_datestamp(Date.today)
    end

    # year, month, and day are integers
    def build_datestamp(year, month, day)
      (year * 100 + month) * 100 + day
    end

    # datestamp is an int of the form yyyymmdd
    def datestamp_to_date(datestamp)
      day = datestamp % 100
      datestamp = datestamp / 100
      month = datestamp % 100
      year = datestamp / 100
      Date.new(year, month, day)
    end

    # timestamp is an int of the form yyyymmddHHMMSS
    def timestamp_to_date(timestamp)
      ts_str = timestamp.to_s
      year = ts_str.slice(0, 4).to_i
      month = ts_str.slice(4, 2).to_i
      day = ts_str.slice(6, 2).to_i
      Date.new(year, month, day)
    end

    # timestamp is an integer of the form yyyymmddHHMMSS
    def timestamp_to_datestamp(timestamp)
      timestamp / 1000000
    end

    # timestamp is an integer of the form yyyymmddHHMMSS
    # return an integer "monthstamp" of the form yyyymm
    def timestamp_to_monthstamp(timestamp)
      timestamp / 100000000
    end

    # returns [year, month, day] of yyyymmdd integer datestamp
    def datestamp_components(datestamp)
      day = datestamp % 100
      datestamp = datestamp / 100
      month = datestamp % 100
      year = datestamp / 100
      [year, month, day]
    end

    # datestamp is an integer of the form yyyymmdd
    # return an integer "monthstamp" of the form yyyymm
    def datestamp_to_monthstamp(datestamp)
      datestamp / 100
    end

    # monthstamp is an integer of the form yyyymm
    # return an array containing the year and month represented by the monthstamp; array is of the form: [yyyy, mm]
    def monthstamp_to_year_month(monthstamp)
      month = monthstamp % 100
      year = monthstamp / 100
      [year, month]
    end

    def date_at_time(date, hour, minute, second)
      DateTime.new(date.year, date.month, date.day, hour, minute, second)
    end

    def date_at_local_time(date, local_time)
      date_at_time(date, local_time.hour, local_time.minute, local_time.second)
    end

    def date_to_datestamp(date)
      build_datestamp(date.year, date.month, date.day)
    end


    def date_series(start_date, end_date, incrementer_fn = ->(date){ date + 1 })
      series = []
      date = start_date
      while date < end_date
        series << date
        date = incrementer_fn.call(date)
      end
      series
    end

    # periodicalDateSeries
    def date_series_inclusive(start_date, end_date, incrementer_fn = ->(date){ date + 1 })
      series = []
      date = start_date
      while date <= end_date
        series << date
        date = incrementer_fn.call(date)
      end
      series
    end

    # previousBusinessDay
    def prior_business_day(date)
      case day_of_week(date)
      when DayOfWeek::Saturday, DayOfWeek::Sunday, DayOfWeek::Monday
        first_weekday_before_date(DayOfWeek::Friday, date)
      else
        date - 1
      end
    end

    # nextBusinessDay
    def next_business_day(date)
      case day_of_week(date)
      when DayOfWeek::Friday, DayOfWeek::Saturday, DayOfWeek::Sunday
        first_weekday_after_date(DayOfWeek::Monday, date)
      else
        date + 1
      end
    end

    # isBusinessDay
    def business_day?(date)
      day_of_week(date) < DayOfWeek::Saturday    # is date Mon/Tue/Wed/Thu/Fri ?
    end

    # nextMonth
    # returns [month, year] representing the month and year following the given month and year
    def next_month(month, year)
      if month == 12
        [1, year + 1]
      else
        [month + 1, year]
      end
    end

    # previousMonth
    # returns [month, year] representing the month and year preceeding the given month and year
    def previous_month(month, year)
      if month == 1
        [12, year - 1]
      else
        [month - 1, year]
      end
    end

    # addMonths
    def add_months(base_month, base_year, month_offset)
      if month_offset >= 0
        month_offset.times.reduce([base_month, base_year]) do |memo, i|
          month, year = *memo
          next_month(month, year)
        end
      else
        (-month_offset).times.reduce([base_month, base_year]) do |memo, i|
          month, year = *memo
          previous_month(month, year)
        end
      end
    end

    # firstDayOfMonth
    def first_day_of_month(year, month)
      Date.new(year, month, 1)
    end

    # daysInMonth
    COMMON_YEAR_DAYS_IN_MONTH = [nil, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    def days_in_month(year, month)
      if month == 2 && ::Date.gregorian_leap?(year)
        29
      else
        COMMON_YEAR_DAYS_IN_MONTH[month]
      end
    end

    def last_day_of_month(year, month)
      Date.new(year, month, days_in_month(year, month))
    end

    # dayOfWeek and dayOfWeekD
    # returns the day of calendar week (1-7, Monday is 1).
    def day_of_week(date)
      date.cwday    # cwday returns the day of calendar week (1-7, Monday is 1).
    end

    def first_day_of_month_at_or_after_date(desired_day_of_month, date)
      if date.day <= desired_day_of_month
        Date.new(date.year, date.month, desired_day_of_month)
      else
        month, year = next_month(date.month, date.year)
        Date.new(year, month, desired_day_of_month)
      end
    end

    def first_day_of_month_at_or_before_date(day_of_month, date)
      if desired_day_of_month <= date.day
        Date.new(date.year, date.month, desired_day_of_month)
      else
        month, year = previous_month(date.month, date.year)
        Date.new(year, month, desired_day_of_month)
      end
    end

    # offsetOfFirstWeekdayInMonth
    # Returns the number of days that must be added to the first day of the given month to arrive at the first
    #   occurrence of the <desired-weekday> in that month; put another way, it returns the number of days
    #   that must be added to the first day of the given month to arrive at the <desired-weekday> in the first
    #   week of that month.
    # The return value will be an integer in the range [0, 6].
    # NOTE: the return value is the result of the following expression:
    #   (desired-weekday - dayOfWeek(year, month, 1) + 7) mod 7
    # desired-weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
    # month is an integer indicating the month, s.t. 1=Jan., 2=Feb., ..., 11=Nov., 12=Dec.
    # year is an integer indicating the year (e.g. 1999, 2010, 2012, etc.)
    # Example:
    #   offset_of_first_weekday_in_month(1, 2, 2012)    ; monday
    #   > 5
    #   offset_of_first_weekday_in_month(3, 2, 2012)    ; wednesday
    #   > 0
    #   offset_of_first_weekday_in_month(5, 2, 2012)    ; friday
    #   > 2
    def offset_of_first_weekday_in_month(desired_weekday, month, year)
      offset_of_first_weekday_in_monthweekday_at_or_after_weekday(desired_weekday, day_of_week(first_day_of_month(year, month)))
    end

    # offsetOfFirstWeekdayAfterWeekday
    # The return value will be an integer in the range [1, 7].
    # desired_weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
    # current_weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
    # Example:
    # offset_of_first_weekday_after_weekday(2, 2) => 7
    # offset_of_first_weekday_after_weekday(5, 2) => 3
    # offset_of_first_weekday_after_weekday(3, 6) => 4
    def offset_of_first_weekday_after_weekday(desired_weekday, current_weekday)
      offset = offset_of_first_weekday_at_or_after_weekday(desired_weekday, current_weekday)
      offset == 0 ? 7 : offset
    end

    # offsetOfFirstWeekdayBeforeWeekday
    # The return value will be an integer in the range [-7, -1].
    # desired_weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
    # current_weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
    # Example:
    # offset_of_first_weekday_before_weekday(2, 2) => -7
    # offset_of_first_weekday_before_weekday(5, 2) => -4
    # offset_of_first_weekday_before_weekday(3, 6) => -3
    def offset_of_first_weekday_before_weekday(desired_weekday, current_weekday)
      offset = offset_of_first_weekday_at_or_before_weekday(desired_weekday, current_weekday)
      offset == 0 ? -7 : offset
    end

    # offsetOfFirstWeekdayAtOrAfterWeekday
    # The return value will be an integer in the range [0, 6].
    # desired_weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
    # current_weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
    def offset_of_first_weekday_at_or_after_weekday(desired_weekday, current_weekday)
      (desired_weekday - current_weekday + 7) % 7
    end

    # offsetOfFirstWeekdayAtOrBeforeWeekday
    # The return value will be an integer in the range [-6, 0].
    # desired_weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
    # current_weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
    def offset_of_first_weekday_at_or_before_weekday(desired_weekday, current_weekday)
      -((current_weekday - desired_weekday + 7) % 7)
    end

    # firstWeekdayAfterDate
    # desired_weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
    # Example:
    # first_weekday_after_date(DayOfWeek::Friday, Date.new(2012, 2, 18)) => #<Date: 2012-02-24 ((2455982j,0s,0n),+0s,2299161j)>
    # first_weekday_after_date(DayOfWeek::Friday, Date.new(2012, 2, 24)) => #<Date: 2012-03-02 ((2455989j,0s,0n),+0s,2299161j)>
    def first_weekday_after_date(desired_weekday, date)
      offset = offset_of_first_weekday_after_weekday(desired_weekday, day_of_week(date))
      date + offset
    end

    # firstWeekdayAtOrAfterDate
    # desired_weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
    # Example:
    # first_weekday_at_or_after_date(DayOfWeek::Friday, Date.new(2012, 2, 18)) => #<Date: 2012-02-24 ((2455982j,0s,0n),+0s,2299161j)>
    # first_weekday_at_or_after_date(DayOfWeek::Friday, Date.new(2012, 2, 24)) => #<Date: 2012-02-24 ((2455982j,0s,0n),+0s,2299161j)>
    def first_weekday_at_or_after_date(desired_weekday, date)
      offset = offset_of_first_weekday_at_or_after_weekday(desired_weekday, day_of_week(date))
      date + offset
    end

    # firstWeekdayBeforeDate
    # desired_weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
    # Example:
    # first_weekday_before_date(DayOfWeek::Friday, Date.new(2012, 3, 2)) => #<Date: 2012-02-24 ((2455982j,0s,0n),+0s,2299161j)>
    # first_weekday_before_date(DayOfWeek::Wednesday, Date.new(2012, 3, 2)) => #<Date: 2012-02-29 ((2455987j,0s,0n),+0s,2299161j)>
    def first_weekday_before_date(desired_weekday, date)
      offset = offset_of_first_weekday_before_weekday(desired_weekday, day_of_week(date))
      date + offset
    end

    # firstWeekdayAtOrBeforeDate
    def first_weekday_at_or_before_date(desired_weekday, date)
      offset = offset_of_first_weekday_at_or_before_weekday(desired_weekday, day_of_week(date))
      date + offset
    end

    # nthWeekdayOfMonth
    # returns a LocalDate representing the nth weekday in the given month.
    # desired-weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
    # month is an integer indicating the month, s.t. 1=Jan., 2=Feb., ..., 11=Nov., 12=Dec.
    # year is an integer indicating the year (e.g. 1999, 2010, 2012, etc.)
    # Example:
    #   nth_weekday_of_month(3, DayOfWeek::Monday, 1, 2012)    ; returns the 3rd monday in January 2012.
    #   => #<Date: 2012-01-16 ((2455943j,0s,0n),+0s,2299161j)>
    #   nth_weekday_of_month(3, DayOfWeek::Monday, 2, 2012)    ; returns the 3rd monday in February 2012.
    #   => #<Date: 2012-02-20 ((2455978j,0s,0n),+0s,2299161j)>
    #   nth_weekday_of_month(1, DayOfWeek::Wednesday, 2, 2012) ; returns the 1st wednesday in February 2012.
    #   => #<Date: 2012-02-01 ((2455959j,0s,0n),+0s,2299161j)>
    def nth_weekday_of_month(n, desired_weekday, month, year)
      nth_weekday_at_or_after_date(n, desired_weekday, first_day_of_month(year, month))
    end

    # returns a Date representing the nth weekday after the given date
    # desired-weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
    # Example:
    # nth_weekday_after_date(1, DayOfWeek::Friday, Date.new(2012, 2, 18)) => #<Date: 2012-02-24 ((2455982j,0s,0n),+0s,2299161j)>
    # nth_weekday_after_date(2, DayOfWeek::Friday, Date.new(2012, 2, 18)) => #<Date: 2012-03-02 ((2455989j,0s,0n),+0s,2299161j)>
    # nth_weekday_after_date(4, DayOfWeek::Wednesday, Date.new(2012, 2, 18)) => #<Date: 2012-03-14 ((2456001j,0s,0n),+0s,2299161j)>
    def nth_weekday_after_date(n, desired_weekday, date)
      week_offset_in_days = 7 * (n - 1)
      first_weekday_after_date(desired_weekday, date) + week_offset_in_days
    end

    # returns a Date representing the nth weekday after the given date
    # desired-weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
    # Example:
    # nth_weekday_at_or_after_date(1, DayOfWeek::Friday, Date.new(2012, 2, 3)) => #<Date: 2012-02-03 ((2455961j,0s,0n),+0s,2299161j)>
    # nth_weekday_at_or_after_date(2, DayOfWeek::Friday, Date.new(2012, 2, 3)) => #<Date: 2012-02-10 ((2455968j,0s,0n),+0s,2299161j)>
    def nth_weekday_at_or_after_date(n, desired_weekday, date)
      week_offset_in_days = 7 * (n - 1)
      first_weekday_at_or_after_date(desired_weekday, date) + week_offset_in_days
    end

    # returns a Date representing the nth weekday after the given date
    # desired-weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
    # Example:
    # nth_weekday_before_date(1, DayOfWeek::Friday, Date.new(2012, 3, 2)) => #<Date: 2012-02-24 ((2455982j,0s,0n),+0s,2299161j)>
    # nth_weekday_before_date(2, DayOfWeek::Friday, Date.new(2012, 3, 2)) => #<Date: 2012-02-17 ((2455975j,0s,0n),+0s,2299161j)>
    # nth_weekday_before_date(4, DayOfWeek::Wednesday, Date.new(2012, 3, 2)) => #<Date: 2012-02-08 ((2455966j,0s,0n),+0s,2299161j)>
    def nth_weekday_before_date(n, desired_weekday, date)
      week_offset_in_days = 7 * (n - 1)
      first_weekday_before_date(desired_weekday, date) - week_offset_in_days
    end

    def nth_weekday_at_or_before_date(n, desired_weekday, date)
      week_offset_in_days = 7 * (n - 1)
      first_weekday_at_or_before_date(desired_weekday, date) - week_offset_in_days
    end

    # Returns a LocalDate representing the last weekday in the given month.
    # desired-weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
    # month is an integer indicating the month, s.t. 1=Jan., 2=Feb., ..., 11=Nov., 12=Dec.
    # year is an integer indicating the year (e.g. 1999, 2010, 2012, etc.)
    # source: http://www.irt.org/articles/js050/
    # formula:
    #   daysInMonth - (DayOfWeek(daysInMonth,month,year) - desiredWeekday + 7)%7
    # Example:
    #   last_weekday(DayOfWeek::Monday, 2, 2012)
    #   => #<Date: 2012-02-27 ((2455985j,0s,0n),+0s,2299161j)>
    def last_weekday(desired_weekday, month, year)
      days = days_in_month(year, month)
      day_of_month = days - (day_of_week(Date.new(year, month, days)) - desired_weekday + 7) % 7
      Date.new(year, month, day_of_month)
    end

    ##################################################################### special dates #####################################################################

    def new_years(year)
      Date.new(year, 1, 1)
    end

    def new_years?(date)
      new_years(date.year) == date
    end

    def martin_luther_king_jr_day(year)
      nth_weekday_of_month(3, DayOfWeek::Monday, Month::January, year)
    end

    def martin_luther_king_jr_day?(date)
      martin_luther_king_jr_day(date.year) == date
    end

    def presidents_day(year)
      nth_weekday_of_month(3, DayOfWeek::Monday, Month::February, year)
    end

    def presidents_day?(date)
      presidents_day(date.year) == date
    end

    def memorial_day(year)
      last_weekday(DayOfWeek::Monday, Month::May, year)
    end

    def memorial_day?(date)
      memorial_day(date.year) == date
    end

    def independence_day(year)
      Date.new(year, Month::July, 4)
    end

    def independence_day?(date)
      independence_day(date.year) == date
    end

    def labor_day(year)
      nth_weekday_of_month(1, DayOfWeek::Monday, Month::September, year)
    end

    def labor_day?(date)
      labor_day(date.year) == date
    end

    def columbus_day(year)
      nth_weekday_of_month(2, DayOfWeek::Monday, Month::October, year)
    end

    def columbus_day?(date)
      columbus_day(date.year) == date
    end

    def thanksgiving(year)
      nth_weekday_of_month(4, DayOfWeek::Thursday, Month::November, year)
    end

    def thanksgiving?(date)
      thanksgiving(date.year) == date
    end

    def christmas(year)
      Date.new(year, Month::December, 25)
    end

    def christmas?(date)
      christmas(date.year) == date
    end

    # This is a non-trivial calculation. See http://en.wikipedia.org/wiki/Computus
    #   "Computus (Latin for "computation") is the calculation of the date of Easter in the Christian calendar."
    #   Evidently the scientific study of computation (or Computer Science, as we like to call it) was born out
    #   of a need to calculate when Easter was going to be.
    # See http://www.linuxtopia.org/online_books/programming_books/python_programming/python_ch38.html
    # The following code was taken from: http://www.merlyn.demon.co.uk/estralgs.txt
    # function McClendon(YR) {
    #   var g, c, x, z, d, e, n
    #   g = YR % 19 + 1   // Golden
    #   c = ((YR/100)|0) + 1    // Century
    #   x = ((3*c/4)|0) - 12    // Solar
    #   z = (((8*c+5)/25)|0) - 5  // Lunar
    #   d = ((5*YR/4)|0) - x - 10 // Letter ?
    #   e = (11*g + 20 + z - x) % 30  // Epact
    #   if (e<0) e += 30    // Fix 9006 problem
    #   if ( ( (e==25) && (g>11) ) || (e==24) ) e++
    #   n = 44 - e
    #   if (n<21) n += 30   // PFM
    #   return n + 7 - ((d+n)%7)  // Following Sunday
    #   }
    def easter(year)
      g = year % 19 + 1
      c = year / 100 + 1
      x = (3 * c / 4) - 12
      z = (8 * c + 5) / 25 - 5
      d = 5 * year / 4 - x - 10
      e = (11 * g + 20 + z - x) % 30
      e1 = e < 0 ? e + 30 : e
      e2 = (e1 == 25 && g > 11) || e1 == 24 ? e1 + 1 : e1
      n = 44 - e2
      n1 = n < 21 ? n + 30 : n
      n2 = (n1 + 7) - ((d + n1) % 7)
      day = n2 > 31 ? n2 - 31 : n2
      month = n2 > 31 ? 4 : 3
      Date.new(year, month, day)
    end

    def easter?(date)
      easter(date.year) == date
    end

    def good_friday(year)
      easter(year) - 2
    end

    def good_friday?(date)
      good_friday(date.year) == date
    end

    HolidayLookupFunctions = [
      ->(date) { new_years?(date) },
      ->(date) { martin_luther_king_jr_day?(date) },
      ->(date) { presidents_day?(date) },
      ->(date) { good_friday?(date) },
      ->(date) { memorial_day?(date) },
      ->(date) { independence_day?(date) },
      ->(date) { labor_day?(date) },
      ->(date) { columbus_day?(date) },
      ->(date) { thanksgiving?(date) },
      ->(date) { christmas?(date) }
    ]
    def holiday?(date)
      HolidayLookupFunctions.any? {|holiday_fn| holiday_fn.call(date) }
    end
  end
end

module DateExtensions
  refine Date do
    def at_time(hour, minute, second)
      Date.date_at_time(self, hour, minute, second)
    end

    def at_local_time(local_time)
      Date.date_at_local_time(self, local_time)
    end

    def to_datestamp
      Date.date_to_datestamp(self)
    end
  end
end
