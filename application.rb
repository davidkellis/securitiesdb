# require_relative 'lib/yahoofinance'
#
# require_relative 'config'
# require_relative 'database'
#
# require_relative 'date'
# require_relative 'stats'
# require_relative 'time'

# require_relative 'downloaders/fundamental_data_downloader'
# require_relative 'downloaders/profile_downloader'
# require_relative 'downloaders/split_history_downloader'
# require_relative 'downloaders/security_downloader'
# require_relative 'protobuf/tradesim.pb'

# require_relative 'importers/import_annual_reports'
# require_relative 'importers/import_dividends'
# require_relative 'importers/import_eod_bars'
# require_relative 'importers/import_profiles'
# require_relative 'importers/import_quarterly_reports'
# require_relative 'importers/import_securities'
# require_relative 'importers/import_splits'



require 'rubygems'
require 'bundler/setup'

require 'logging'
require 'quandl'

require_relative 'app_config'
require_relative 'app/database'

class Application
  DEFAULT_CONFIG_FILE_PATH = "config/application.yml"

  class << self
    def logger
      @logger || configure_logger
    end

    def database_logger
      @database_logger || configure_database_logger
    end

    def load_config(config_file_path = DEFAULT_CONFIG_FILE_PATH)
      AppConfig.load(config_file_path)
    end

    def load(config_file_path = DEFAULT_CONFIG_FILE_PATH)
      load_config(config_file_path)

      configure_root_logger
      configure_logger
      configure_database_logger

      configure_quandl

      Database.connect(AppConfig.database.connection_string, database_logger)

      require_files
    end

    def configure_root_logger
      Logging.logger.root.level = :error
      Logging.logger.root.appenders = if AppConfig.log_file
        Logging.appenders.file(AppConfig.log_file)
      else
        Logging.appenders.stdout
      end
    end

    def configure_logger
      @logger = Logging.logger['default']
      @logger.level = AppConfig.log_level ? AppConfig.log_level.to_sym : :info
      @logger
    end

    def configure_database_logger
      @database_logger = Logging.logger['database']
      @database_logger.level = AppConfig.database_log_level ? AppConfig.database_log_level.to_sym : :warn
      @database_logger
    end

    def configure_quandl
      Quandl::ApiConfig.api_key = AppConfig.quandl.api_key
      Quandl::ApiConfig.api_version = AppConfig.quandl.api_version
    end

    def require_files
      require_relative 'app/data_model'
      require_relative 'app/date'
      require_relative 'app/stats'
      require_relative 'app/time'

      require_relative 'app/clients/bsym'
      require_relative 'app/clients/csidata'
      require_relative 'app/clients/quandl_eod'
      # require_relative 'app/clients/quandl_fundamentals'
      require_relative 'app/clients/yahoofinance'
      require_relative 'app/importers/bsym_exchanges'
      require_relative 'app/importers/bsym_securities'
      require_relative 'app/importers/csidata'
      require_relative 'app/importers/quandl_eod'
      # require_relative 'app/importers/quandl_fundamentals'
      # require_relative 'app/importers/yahoo_eod'
      # require_relative 'app/importers/yahoo_splits_and_dividends'
    end

  end
end
