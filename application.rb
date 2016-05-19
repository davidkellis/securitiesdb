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

      configure_security_name_database_factory
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
      require_relative 'app/time_series_map_loader'
      require_relative 'app/corporate_action_loader'
      require_relative 'app/data_model'
      require_relative 'app/date'
      require_relative 'app/eod_bar_loader'
      require_relative 'app/lru_cache'
      require_relative 'app/security_classification_loader'
      require_relative 'app/security_name_database'
      require_relative 'app/stats'
      require_relative 'app/time_series_map'
      require_relative 'app/time_series_observation_loader'
      require_relative 'app/time'

      require_relative 'app/clients/bsym'
      require_relative 'app/clients/csidata'
      require_relative 'app/clients/quandl_eod'
      require_relative 'app/clients/quandl_fundamentals'
      require_relative 'app/clients/yahoofinance'

      require_relative 'app/domain/corporate_action_adjustment'
      require_relative 'app/domain/create_security'
      require_relative 'app/domain/currency'
      require_relative 'app/domain/find_eod_bar'
      require_relative 'app/domain/find_fundamentals'
      require_relative 'app/domain/find_corporate_action'
      require_relative 'app/domain/find_security'
      require_relative 'app/domain/find_security_classification'
      require_relative 'app/domain/find_time_series'
      require_relative 'app/domain/lookup_fundamentals'
      require_relative 'app/domain/time_zone'

      require_relative 'app/importers/quandl_time_series_importer'
      # require_relative 'app/importers/bsym_exchanges'
      # require_relative 'app/importers/bsym_securities'
      require_relative 'app/importers/csidata'
      require_relative 'app/importers/exchanges'
      require_relative 'app/importers/optiondata'
      require_relative 'app/importers/quandl_bls'
      require_relative 'app/importers/quandl_cme'
      require_relative 'app/importers/quandl_eod'
      require_relative 'app/importers/quandl_fed'
      require_relative 'app/importers/quandl_fred'
      require_relative 'app/importers/quandl_fundamentals'
      require_relative 'app/importers/quandl_us_census'
      require_relative 'app/importers/quandl_us_treasury'
      require_relative 'app/importers/yahoo_eod'
      require_relative 'app/importers/yahoo_splits_and_dividends'
    end

    def configure_security_name_database_factory
      SecurityNameDatabaseFactory.configure(AppConfig.company_name_search_database_dir)
    end

  end
end
