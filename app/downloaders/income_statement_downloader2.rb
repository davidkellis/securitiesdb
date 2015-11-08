require 'pp'
require 'watir-webdriver'
require 'fileutils'

BASE_URI = "http://financials.morningstar.com/income-statement/is.html?t=<TICKER>&region=USA&culture=en-US"

REPORT_TYPES = {
    :annual => "Annual",
    :quarterly => "Quarterly"
}

TableRow = Struct.new(:header, :data)

def open_browser
  Watir::Browser.new :firefox
end

# report_type must be either :annual or :quarterly
def download_income_statements_from_morningstar(tickers, report_type = :annual)
  puts "Downloading income statement history from Morningstar."

  b = open_browser

  tickers.each do |ticker|
    puts "Processing #{ticker}"

    begin
      # open the income statement page for <ticker>
      uri = BASE_URI.gsub("<TICKER>", ticker)
      b.goto uri

      # select the type of report we want to download - annual or quarterly
      dropdown_menu = b.li :id => "menu_A"
      dropdown_menu.click
      report_type_link = dropdown_menu.link :text => REPORT_TYPES[report_type]
      report_type_link.click

      # locate the button for rounding the figures down one order of magnitude
      round_down_button = b.link :class => "rf_rounddn"

      # click the round-down button until we can't click it anymore - that means we've rounded down as far as possible
      while round_down_button.exists?
        round_down_button.click
      end

      # begin extracting content from the income statement table
      snapshots = extract_snapshots(b)
    rescue Exception => e
      puts "Error while processing #{ticker} - #{e.message}"
      next
    end
  end

  b.quit
end

def extract_snapshots(browser)
  left_table = browser.div :class => "rf_table_left"
  right_table = browser.div :class => "rf_table"

  right_table_header_row = right_table.div :class => "rf_header"

  observation_date_divs = right_table_header_row.divs :class => "year"
  observation_div_id_to_date = Hash[ observation_date_divs.map{|column_header_cell| [column_header_cell.id, column_header_cell.text.strip] } ]
  observation_dates = observation_div_id_to_date.values

  # select only the visible row header divs [that aren't just used for padding/spacing]
  row_header_divs = left_table.divs(:id => /label_/).select{|div| div.visible? }.reject {|div| div.id.end_with? "_padding" }
  row_header_id_to_label = Hash[ row_header_divs.map do |row_header_div|
    label = row_header_div.text.strip
    if label.end_with? "..."
      lbl_div = row_header_div.div(:class => "lbl")
      label = lbl_div.attribute_value("title")
    end
    [row_header_div.id, label]
  end ]
  row_header_ids = row_header_id_to_label.keys    # looks like: ["label_g1", "label_i2", "label_i3", ...]

  pp row_header_id_to_label

  data_row_divs = right_table.divs(:id => /data_/)
  data_row_id_to_div = Hash[ data_row_divs.map{|data_row_div| [data_row_div.id, data_row_div] } ]
  data_row_ids = row_header_ids.map {|label_id| label_id.sub("label", "data") }
  ordered_data_row_divs = data_row_ids.map {|data_row_id| data_row_id_to_div[data_row_id] }

  table_rows = row_header_divs.zip(ordered_data_row_divs).map{|pair| TableRow.new(pair.first, pair.last) }

  snapshots = Hash[ observation_dates.map {|observation_date| [observation_date, {:observation_date => observation_date}] } ]       # i.e. snapshots = {"2001-12" => {:observation_date => "2001-12"}, ...}

  table_rows.each do |table_row|
    header_div = table_row.header
    data_div = table_row.data

    row_header_label = row_header_id_to_label[header_div.id]

    data_cells = data_div.divs
    raise "Row contains too many cells" unless data_cells.count == observation_dates.count

    data_cells.each do |data_cell_div|
      observation_div_id = data_cell_div.id
      observation_date = observation_div_id_to_date[observation_div_id]

      observation_value = data_cell_div.text.strip.gsub(",", "")

      snapshots[observation_date][row_header_label] = observation_value
    end
  end

  pp snapshots
  snapshots
end

def main
  tickers = if ARGV.length == 0
              %w{AAPL F VFINX}
            elsif ARGV.length == 1 && ARGV[0].index(/\.\w+/)   # treat as filename
              File.readlines(ARGV[0]).map{|line| line.strip }
            else
              ARGV
            end

  report_type = :annual

  FileUtils.mkdir("#{report_type}_income_statements") unless File.exists?("#{report_type}_income_statements")

  # grab only tickers that are traded on primary american exchanges - those without an exchange suffix (e.g. ticker.OB)
  tickers = tickers.select {|ticker| ticker.index(".").nil? }
  puts "#{tickers.count} symbols"

  download_income_statements_from_morningstar(tickers, report_type)
end

main