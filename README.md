securitiesdb
============

## Setup

Prerequisites:
- Install Firefox (the Bsym library screen scrapes http://bsym.bloomberg.com/sym/ using the watir-webdriver gem + Firefox)
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
