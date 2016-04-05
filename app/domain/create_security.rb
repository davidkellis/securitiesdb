require 'singleton'

class CreateSecurity
  include Singleton

  class << self
    extend Forwardable
    def_delegators :instance, :run
  end


  def run(name, security_type_name, industry_name = nil, sector_name = nil)
    security_type = find_or_create_security_type(security_type_name)
    industry = find_or_create_industry(industry_name)
    sector = find_or_create_sector(sector_name)

    security = Security.create(
      security_type_id: security_type && security_type.id,
      industry_id: industry && industry.id,
      sector_id: sector && sector.id,
      name: name,
      search_key: extract_search_key_from_security_name(name)
    )

    db = SecurityNameDatabaseRegistry.get(security_type_name)
    search_key = extract_search_key_from_security_name(name)
    db.add(search_key)

    security
  end

  private

  def extract_search_key_from_security_name(security_name)
    security_name.downcase
  end

  def find_or_create_security_type(security_type_name)
    if security_type_name && !security_type_name.empty?
      SecurityType.first(name: security_type_name) || SecurityType.create(name: security_type_name)
    end
  end

  def find_or_create_sector(sector_name)
    if sector_name && !sector_name.empty?
      Sector.first(name: sector_name) || Sector.create(name: sector_name)
    end
  end

  def find_or_create_industry(industry_name)
    if industry_name && !industry_name.empty?
      Industry.first(name: industry_name) || Industry.create(name: industry_name)
    end
  end

end
