require 'settingslogic'
require 'uri'

class AppConfig < Settingslogic
  def self.load(config_file_path)
    source config_file_path
    # namespace environment
    suppress_errors true
    load!

    # connection_string = self.database.connection_string
    # uri = URI(connection_string) if connection_string
    # self["database_adapter"] = uri ? uri.scheme : nil
  end
end
