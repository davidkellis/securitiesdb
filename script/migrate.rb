require_relative '../application'

def migrate
  `sequel -m migrations/ #{AppConfig.database.connection_string}`
end

def main
  Application.load_config(ARGV.first || Application::DEFAULT_CONFIG_FILE_PATH)
  migrate
end

main if __FILE__ == $0
