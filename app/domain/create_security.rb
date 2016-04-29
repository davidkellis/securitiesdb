require 'singleton'

class CreateSecurity
  include Singleton

  class << self
    extend Forwardable
    def_delegators :instance, :run
  end


  def run(name, security_type_name)
    security_type = find_or_create_security_type(security_type_name)

    security = Security.create(
      security_type_id: security_type && security_type.id,
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

end
