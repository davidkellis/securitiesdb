securitiesdb
============

## Setup

Prerequisites:
- Install Firefox (the Bsym library screen scrapes http://bsym.bloomberg.com/sym/ using the watir-webdriver gem + Firefox)
- Install Postgres libraries so that step 2 can install the pg gem.


1. Install ruby 2.2.3
   ```
   rbenv install 2.2.3
   ```

2. Install bundler (if not already installed)
   ```
   gem install bundler
   ```
3. bundle install
4. Change database connection string in application.yml
5. Setup Database
   ```
   script/setup_db
   ```

6. Import data

   To import all data:
   ```
   script/import --all
   OR
   script/import -b -c --quandl-eod --quandl-fundamentals
   OR
   script/import --bsym --csi --quandl-eod --quandl-fundamentals
   ```

   To import only exchanges:
   ```
   script/import -e
   OR
   script/import --exchanges
   ```

## Reset Database

```
script/reset_db
```

## Misc

counts = Fund.all.map{|f| f.eod_bars.count }.reject{|count| count == 0}; counts.reduce(&:+) / counts.count
