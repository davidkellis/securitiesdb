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
   ```
   script/import -b -c
   ```

## Reset Database

```
script/reset_db
```

## Misc

counts = Fund.all.map{|f| f.eod_bars.count }.reject{|count| count == 0}; counts.reduce(&:+) / counts.count
