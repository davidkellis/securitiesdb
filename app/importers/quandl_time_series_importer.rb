require 'socket'    # for SocketError

class QuandlTimeSeriesImporter
  def initialize
    @data_vendor = DataVendor.first(name: "Quandl")
  end

  def import
    raise "QuandlTimeSeriesImporter#import not implemented."

    # Expected implementation would be along the following lines:

    # BLSE/CEU0000000001 -> Employment - All employees, thousands; Total nonfarm industry
    # import_quandl_time_series("BLSE", "CEU0000000001")

    # import_quandl_time_series_database("BLSE")    # BLS Employment & Unemployment - https://www.quandl.com/data/BLSE
  end

  protected

  def import_quandl_time_series_database(quandl_database_code)
    datasets = get_datasets(quandl_database_code)
    datasets.each {|dataset| import_quandl_dataset(dataset) }
  end

  def import_quandl_time_series(quandl_database_code, quandl_dataset_code)
    quandl_code = "#{quandl_database_code}/#{quandl_dataset_code}"
    dataset = get_dataset(quandl_code)
    import_quandl_dataset(dataset)
  end

  # quandl_dataset_code is a name like "BLSE/CEU0000000001"
  def get_dataset(quandl_dataset_code)
    Quandl::Dataset.get(quandl_dataset_code)
  end

  # quandl_database_code is a name like "BLSE"
  def get_datasets(quandl_database_code)
    db = Quandl::Database.get(quandl_database_code)
    total_pages = db.datasets.meta[:total_pages]
    (1..total_pages).reduce([]) do |memo, page_number|
      i = 1
      begin
        datasets = db.datasets(params: {page: page_number}).values
        memo.concat(datasets)
      rescue SocketError => e
        puts "Connection error ##{i}"
        puts e.message
        puts e.backtrace("\n")
        i += 1
        retry if i <= 10
      rescue => e
        puts "Error ##{i}"
        puts e.message
        puts e.backtrace("\n")
        i += 1
        retry if i <= 10
      end
      memo
    end
  end

  def import_quandl_dataset(dataset)
    log("Importing Quandl dataset #{dataset.database_code}/#{dataset.dataset_code} - #{dataset.name}.")
    update_frequency = UpdateFrequency.lookup(dataset.frequency) || UpdateFrequency.irregular
    ts = lookup_time_series(@data_vendor, dataset.database_code, dataset.dataset_code) ||
           create_time_series(@data_vendor, update_frequency, dataset.database_code, dataset.dataset_code, dataset.name, dataset.description)
    if ts
      observation_model_class = case dataset.frequency
        when "daily"
          DailyObservation
        when "weekly"
          WeeklyObservation
        when "monthly"
          MonthlyObservation
        when "quarterly"
          QuarterlyObservation
        when "annual"
          YearlyObservation
        else
          quandl_code = "#{dataset.database_code}/#{dataset.dataset_code}"
          raise "Unknown frequency, #{dataset.frequency}, referenced in Quandl dataset \"#{quandl_code}\"."
      end

      retries = 5
      data_records = begin
        dataset.data
      rescue => e
        if retries > 0
          retries -= 1
          log("Dataset #{dataset.database_code}/#{dataset.dataset_code} - #{dataset.name} cannot be downloaded right now. Retrying in 5 seconds. Error #{e.message}\nBacktrace: #{e.backtrace.join("\n")}")
          retry
        else
          log("Dataset #{dataset.database_code}/#{dataset.dataset_code} - #{dataset.name} failed to download: #{e.message}\nBacktrace: #{e.backtrace.join("\n")}")
          return nil
        end
      end
      data_records.each do |record|
        case record.column_names
        when ["Date", "Value"]
          date = convert_date(record.date)
          create_observation(observation_model_class, ts.id, date, record.value) unless lookup_observation(observation_model_class, ts.id, date)
        when ["Period", "Value"]
          date = convert_date(record.period)
          create_observation(observation_model_class, ts.id, date, record.value) unless lookup_observation(observation_model_class, ts.id, date)
        else
          log("Dataset #{dataset.database_code}/#{dataset.dataset_code} - #{dataset.name} has an unexpected set of column names: #{record.column_names.inspect}")
          return nil
        end
      end
    end
  end

  def lookup_time_series(data_vendor, database, dataset)
    TimeSeries.first(
      data_vendor_id: data_vendor.id,
      database: database,
      dataset: dataset
    )
  end

  def create_time_series(data_vendor, update_frequency, database, dataset, name, description = nil)
    TimeSeries.create(
      data_vendor_id: data_vendor.id,
      update_frequency_id: update_frequency.id,
      database: database,
      dataset: dataset,
      name: name,
      description: description
    )
  end

  # date is a Date object
  # returns the integer yyyymmdd representation of the given Date object
  def convert_date(date)
    date.strftime("%Y%m%d").to_i if date
  end

  def lookup_observation(observation_model_class, time_series_id, datestamp)
    observation_model_class.first(time_series_id: time_series_id, date: datestamp)
  end

  def create_observation(observation_model_class, time_series_id, datestamp, value)
    observation_model_class.create(time_series_id: time_series_id, date: datestamp, value: value)
  end

  def log(msg)
    Application.logger.info("#{Time.now} - #{msg}")
  end

end
