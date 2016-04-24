class TimeSeriesTable
  def initialize
    @variables = []
  end

  def add_column(variable)
    @variables << variable
  end

  # blk is a block that takes 2 arguments: time and row
  def each_nonfiltered_row(time_series, slice_size = 30, &blk)
    if block_given?
      time_series.each_slice(slice_size).each do |time_series_slice|
        time_series_slice.each do |time|
          row = @variables.map {|variable| variable.observe(time) }
          blk.call(time, row)
        end
      end
    else
      enum_for(:each_nonfiltered_row, time_series, slice_size)
    end
  end

  # same as #each_nonfiltered_row except that rows containing any nil values are filtered out
  # blk is a block that takes 2 arguments: time and row
  def each_filtered_row(time_series, slice_size = 30, &blk)
    if block_given?
      each_nonfiltered_row(time_series, slice_size) do |time, row|
        blk.call(time, row) unless row.any?(&:nil?)
      end
    else
      enum_for(:each_filtered_row, time_series, slice_size)
    end
  end

  # row_enum_fn is either :each_nonfiltered_row, or :each_filtered_row
  # blk is a block that takes 1 argument: row
  def each_row(time_series, include_column_headers = true, include_date_column = true, row_enum_fn = :each_filtered_row, &blk)
    if block_given?
      if include_column_headers
        column_headers = @variables.map(&:name)
        column_headers.unshift("Date") if include_date_column
        blk.call(column_headers)
      end
      self.send(row_enum_fn, time_series) do |time, row|
        row.unshift(time) if row && include_date_column
        blk.call(row)
      end
    else
      enum_for(:each_row, time_series, include_column_headers, include_date_column, row_enum_fn)
    end
  end

  # row_enum_fn is either :each_nonfiltered_row, or :each_filtered_row
  def to_a(time_series, include_column_headers = true, include_date_column = true, row_enum_fn = :each_filtered_row)
    each_row(time_series, include_column_headers, include_date_column, row_enum_fn).to_a
  end

  # row_enum_fn is either :each_nonfiltered_row, or :each_filtered_row
  def save_csv(filepath, time_series, include_column_headers = true, include_date_column = true, row_enum_fn = :each_filtered_row)
    File.open(filepath, "w+") do |f|
      first_row = include_column_headers
      each_row(time_series, include_column_headers, include_date_column, row_enum_fn) do |row|
        if first_row
          # per page 2 of RFC-4180, "If double-quotes are used to enclose fields, then a double-quote appearing inside
          # a field must be escaped by preceding it with another double quote."
          header_row = row.
            map {|col_value| col_value.gsub('"', '""') }.   # escape each double quote with a preceeding double quote
            map {|col_value| "\"#{col_value}\"" }.          # enclose each header field within double quotes
            join(',')
          f.puts(header_row)
          first_row = false
        else
          f.puts(row.join(','))
        end
      end
    end
  end
end
