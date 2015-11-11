require 'date'
require 'pp'

require_relative "../clients/csidata"

class CsiDataImporter
  attr_accessor :csi_client

  def initialize
    self.csi_client = CsiData::Client.new
  end

  def log(msg)
    Application.logger.info(msg)
  end

  def import(exchanges_to_import = Exchange.us_exchanges)
  end

end
