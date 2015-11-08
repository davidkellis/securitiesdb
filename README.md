securitiesdb
============

## Setup

1. Install ruby 2.2.3
   ```
   rbenv install 2.2.3
   ```

2. Install bundler (if not already installed)
   ```
   gem install bundler
   ```
3. bundle install
4. Change database connection string in config.yml
5. Setup Database
   ```
   [./drop_db]
   ./setup_db
   ```

6. Import data
   ```
   script/import
   ```
   or
   ```
   ruby importers/import_securities.rb
   ruby importers/import_eod_bars.rb [ticker1 ticker2 ...]
   ruby importers/import_dividends_and_splits.rb [ticker1 ticker2 ...]
   ```

   Example:

   bundle exec ruby importers/import_eod_bars.rb VFINX MIDHX RERCX ODVNX OIBNX WMGRX SAMVX TRLGX PSSMX PLFMX CMPIX PRRRX PTRRX FSIAX

   bundle exec ruby importers/import_dividends_and_splits.rb VFINX MIDHX RERCX ODVNX OIBNX WMGRX SAMVX TRLGX PSSMX PLFMX CMPIX PRRRX PTRRX FSIAX


## Reset Database

delete from sampling_distributions;
delete from trials;
delete from securities_trial_sets;
delete from trial_set_distributions;
delete from trial_sets;


## Misc

counts = Fund.all.map{|f| f.eod_bars.count }.reject{|count| count == 0}; counts.reduce(&:+) / counts.count
