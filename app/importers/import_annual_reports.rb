require 'date'
require 'json'
require 'pp'
require_relative '../application'

class AnnualReportImporter
  DEFAULT_START_DATE = Date.parse("19500101")
  START_OF_BUSINESS_HOURS = "080000"
  END_OF_BUSINESS_HOURS = "170000"
  END_OF_TRADING_DAY = "160000"  # market open from 9:30 a.m. to 4:00 p.m. eastern time

  def import_records(securities)
    downloader = FundamentalDataDownloader.new

    for security in securities
      most_recent_record = find_most_recent_record(security)
      start_date = compute_start_date(most_recent_record)
      new_records = download_data(downloader, security, start_date)
      save_records(new_records)
      security.reload
    end
  end

  def find_most_recent_record(security)
    security.annual_reports.first(:order => [:end_time.desc])
  end

  def compute_start_date(most_recent_record)
    if most_recent_record
      extract_date(most_recent_record.end_time) + 1
    else
      DEFAULT_START_DATE
    end
  end

  def extract_date(timestamp)
    Date.parse(timestamp.to_s[0...8])
  end

  def download_data(downloader, security, start_date)
    ticker = security.symbol
    end_date = Date.today()

    if start_date <= end_date
      #puts "#{ticker}:\t#{start_date} to #{end_date}"

      annual_reports = downloader.download_financial_reports_within_date_range([ticker], start_date, end_date, report_type = :annual, random_delay_range = 2..10)[ticker]

      build_records(security, annual_reports)
    else
      []
    end
  end

  # annual_reports is a structure of the form:
  # [
  #   {
  #       :report_end_date => "20050331",
  #       :report_publication_date => "20050414",
  #       :income_statement => {...},
  #       :balance_sheet => {...},
  #       :cash_flow_statement => {...}
  #   },
  #   ...
  # ]
  def build_records(security, annual_reports)
    annual_reports.map do |report|
      end_date = Date.parse(report[:report_end_date])
      start_date = compute_report_start_date(end_date)
      start_time = start_date.strftime("%Y%m%d#{START_OF_BUSINESS_HOURS}")
      end_time = end_date.strftime("%Y%m%d#{END_OF_BUSINESS_HOURS}")
      publication_time = format_datestamp_as_timestamp(report[:report_publication_date], END_OF_TRADING_DAY)
      income_statement = JSON.generate(report[:income_statement])
      balance_sheet = JSON.generate(report[:balance_sheet])
      cash_flow_statement = JSON.generate(report[:cash_flow_statement])
      build_record(security, start_time, end_time, publication_time, income_statement, balance_sheet, cash_flow_statement)
    end
  end

  def compute_report_start_date(report_end_date)
    (report_end_date << 3) + 1
  end

  def format_datestamp_as_timestamp(datestamp, time)
    datestamp + time
  end

  def build_record(security, start_time, end_time, publication_time, income_statement, balance_sheet, cash_flow_statement)
    AnnualReport.new(:security_id => security.id,
                     :start_time => start_time.to_i,
                     :end_time => end_time.to_i,
                     :publication_time => publication_time.to_i,
                     :income_statement => income_statement,
                     :balance_sheet => balance_sheet,
                     :cash_flow_statement => cash_flow_statement)
  end

  def save_records(records)
    puts "#{records.count} new records"
    record_being_processed = nil
    records.each do |record|
      record_being_processed = record
      record_being_processed.save
    end
  rescue => e
    puts "Unable to save annual report: #{record_being_processed.values.to_s}"
    puts ">> #{e.message}"
  end
end

def main
  Database.connect

  securities = Stock.amex + Stock.nasdaq + Stock.nyse

  AnnualReportImporter.new.import_records(securities)
end

main if __FILE__ == $0