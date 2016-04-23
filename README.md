securitiesdb
============

This project populates a local Postgres database with security symbols, un-adjusted EOD data, splits, dividends, company fundamentals, and options data. Economic indicators - employment and labor statistics, inflation rates, interest rates, exchange rates, imports/exports, domestic production stats, treasury rates, debt, tax revenues, etc. - are also available.

Michael Halls-Moore, the guy behind [quantstart.com](https://www.quantstart.com), described and popularized the idea of a securities master database in his two
excellent articles - [(1) Securities Master Databases for Algorithmic Trading](https://www.quantstart.com/articles/Securities-Master-Databases-for-Algorithmic-Trading) and [(2) Securities Master Database with MySQL and Python](https://www.quantstart.com/articles/Securities-Master-Database-with-MySQL-and-Python). This project is an implementation of his idea of a securities master database.

- Symbols are sourced from CSI Data. (see http://www.csidata.com/?page_id=10 ; stocks: http://www.csidata.com/factsheets.php?type=stock&format=html, indices: http://www.csidata.com/factsheets.php?type=stock&format=html&exchangeid=81, etc.)
- EOD data, splits, and dividends are sourced from Quandl's $50/month EOD database. (see https://www.quandl.com/data/EOD/)
- Fundamental data is sourced from Quandl's $150/quarter SF1 database. (see https://www.quandl.com/data/SF1/)
- Employment, inflation and prices, pay and benefits, and productivity data are sourced from Quandl's free BLSE, BLSI, BLSB, and BLSP databases. (see https://www.quandl.com/data/BLSE, https://www.quandl.com/data/BLSI, https://www.quandl.com/data/BLSB, and https://www.quandl.com/data/BLSP)
- Growth, employment, inflation, labor, manufacturing and other US economic statistics are sourced from Quandl's free FRED database. (see https://www.quandl.com/data/FRED)
- Official US figures on money supply, interest rates, mortgages, government finances, bank assets and debt, exchange rates, industrial production are sourced from Quandl's free FED database (see https://www.quandl.com/data/FED)
- US economic stats, imports/exports, domestic production, etc. are sourced from Quandl's USCENSUS database. (see https://www.quandl.com/data/USCENSUS)
- US Treasury rates, yield curve rates, debt, tax revenues, etc. are sourced from Quandl's USTREASURY database. (see https://www.quandl.com/data/USTREASURY)
- Historical options data is sourced from OptionData.net (see http://optiondata.net/)

## Getting Started

1. ```git clone``` this project
   ```
   git clone https://github.com/davidkellis/securitiesdb.git
   ```

2. [Optional] If you want any data from Quandl.com, create an account at [Quandl.com](https://www.quandl.com/) and configure project with your Quandl API key and version information:
   1. Register for an account at Quandl.com
   2. Look up your API key:
      1. Go to Account Settings at https://www.quandl.com/account
      2. Click the API Key link in the navigation bar on the left-hand side of the Account Settings page
      3. Note the API Key and version information
   3. Create a config/application.yml file by copying the config/application.sample.yml file into config/application.yml
   4. Open config/application.yml and change the lines that read:
      ```
      quandl:
         api_key: abc123
         api_version: "2015-04-09"
      ```
      so that they api_key and api_version match the values you noted from Quandl.com
   5. Save your changes to config/application.yml

3. [Optional] If you want any historical options data from OptionData.net:
   1. Place an order for data
   2. Download the zipped data sets from the link they send you by e-mail
   3. Copy the downloaded zip files into the data/ directory within the securitiesdb project directory

4. Install Postgres libraries so that step 7 can install the pg gem (this is only applicable if using MRI, as JRuby doesn't need the pg gem).

5. Install Ruby or JRuby
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

6. Install bundler (if not already installed)
   ```
   gem install bundler
   ```

7. bundle install
   ```
   bundle install
   ```

8. Change database connection string in application.yml

9. Setup Database
   ```
   script/setup_db
   ```

10. Import data

   Import all data
   ```
   script/import --all
   ```

   Import all stock symbols, EOD prices, fundamentals
   ```
   script/import --csi --quandl-eod --quandl-fundamentals
   ```

   Import all stock symbols and options
   ```
   script/import --csi -o data/options2006.zip
   ```

## Reset Database

```
script/reset_db
```
