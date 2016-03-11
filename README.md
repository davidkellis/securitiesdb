securitiesdb
============

This project populates a local postgres database with security symbols, un-adjusted EOD data, splits, dividends, and fundamental data.

- Symbols are sourced from CSI Data. (see http://www.csidata.com/?page_id=10 ; stocks: http://www.csidata.com/factsheets.php?type=stock&format=html, indices: http://www.csidata.com/factsheets.php?type=stock&format=html&exchangeid=81, etc.)
- EOD data, splits, and dividends are sourced from Quandl's $50/month EOD database. (see https://www.quandl.com/data/EOD/)
- Fundamental data is sourced from Quandl's $150/quarter SF1 database. (see https://www.quandl.com/data/SF1/)
- Employment, inflation and prices, pay and benefits, and productivity data are sourced from Quandl's free BLSE, BLSI, BLSB, and BLSP databases. (see https://www.quandl.com/data/BLSE, https://www.quandl.com/data/BLSI, https://www.quandl.com/data/BLSB, and https://www.quandl.com/data/BLSP)
- Growth, employment, inflation, labor, manufacturing and other US economic statistics are sourced from Quandl's free FRED database. (see https://www.quandl.com/data/FRED)
- Official US figures on money supply, interest rates, mortgages, government finances, bank assets and debt, exchange rates, industrial production are sourced from Quandl's free FED database (see https://www.quandl.com/data/FED)
- US economic stats, imports/exports, domestic production, etc. are sourced from Quandl's USCENSUS database. (see https://www.quandl.com/data/USCENSUS)
- US Treasury rates, yield curve rates, debt, tax revenues, etc. are sourced from Quandl's USTREASURY database. (see https://www.quandl.com/data/USTREASURY)

## Setup

Prerequisites:
- Install Postgres libraries so that step 2 can install the pg gem (this is only applicable if using MRI, as JRuby doesn't need the pg gem).


1. Install Ruby or JRuby
   ```
   rbenv install 2.2.3
   ```
   OR
   ```
   rbenv install jruby-9.0.4.0
   ```

   Set the JRUBY_OPTS environment variable in your ~/.bash_profile to a few GB of memory:
   ```
   export JRUBY_OPTS=-J-Xmx8g
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
   script/import -c --quandl-eod --quandl-fundamentals
   OR
   script/import --csi --quandl-eod --quandl-fundamentals
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
