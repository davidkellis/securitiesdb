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

require 'logger'
require 'quandl'

require_relative 'app_config'
require_relative 'app/database'

class Application
  DEFAULT_CONFIG_FILE_PATH = "config/application.yml"

  LOG_LEVEL_MAP = {
    :unknown => Logger::UNKNOWN,
    :fatal => Logger::FATAL,
    :error => Logger::ERROR,
    :warn => Logger::WARN,
    :info => Logger::INFO,
    :debug => Logger::DEBUG
  }

  class << self
    def logger
      @logger || configure_logger
    end

    def load_config(config_file_path = DEFAULT_CONFIG_FILE_PATH)
      AppConfig.load(config_file_path)
    end

    def load(config_file_path = DEFAULT_CONFIG_FILE_PATH)
      load_config(config_file_path)

      configure_logger

      configure_quandl

      Database.connect(AppConfig.database.connection_string, logger)

      require_files
    end

    def configure_logger
      @logger = if AppConfig.log_file
        Logger.new(AppConfig.log_file)
      else
        Logger.new(STDOUT)
      end

      @logger.level = LOG_LEVEL_MAP[AppConfig.log_level.to_sym] || Logger::INFO

      @logger
    end

    def configure_quandl
      Quandl::ApiConfig.api_key = AppConfig.quandl[:api_key]
      Quandl::ApiConfig.api_version = AppConfig.quandl.api_version
    end

    def require_files
      require_relative 'app/data_model'
      require_relative 'app/date'
      require_relative 'app/stats'
      require_relative 'app/time'

      require_relative 'app/clients/bsym'
      require_relative 'app/clients/csidata'
      require_relative 'app/clients/yahoofinance'
      require_relative 'app/importers/bsym_exchanges'
      require_relative 'app/importers/bsym_securities'
      require_relative 'app/importers/csidata'
    end

  end
end
