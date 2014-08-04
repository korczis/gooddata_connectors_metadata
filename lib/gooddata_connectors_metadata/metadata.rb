module GoodDataConnectorsMetadata

  class Metadata

    include Mongo

    def initialize(options = {})
      connect(options)
      @global_hash = {}
      @hash = {}
      # Lets load configuration files
      @hash["configuration"] = Configuration.load_from_schedule(options)
      if (!options["configuration_folder"].nil?)
        @hash["configuration"].merge!(Configuration.load_from_files(options["configuration_folder"]))
      end
    end

    # This method should be called from client, anytime the metadata (global) are changed
    def store_global_hash(schedule_id, options = {})
      db_collection_param = options["db_collection"]
      raise MetadataException, "You have not specified the database collection" if db_collection_param.nil? or db_collection_param.empty?
      db_collection = @db[db_collection_param]
      raise MetadataExpcetion, "The schedule_id is null, cannot generate database key" if schedule_id.nil?
      db_key = schedule_id
      record = db_collection.find({"_id" => db_key}).limit(1)
      if (record.count == 0)
        hash_for_storage = {"_id" => db_key, "created_at" => Time.now.utc,"updated_at" => Time.now.utc, "global_metadata" => @global_hash }
        db_collection.insert(hash_for_storage)
      else
        hash_for_storage = record.first
        hash_for_storage["updated_at"]  = Time.now.utc
        hash_for_storage["metadata"]    = @global_hash
        db_collection.update({"_id" => db_key},hash_for_storage)
      end

    end

    # This method should be called from client, anytime the metadata (per execution) are changed
    def store_hash(schedule_id,execution_id,options = {})
      db_collection_param = options["db_collection"]
      raise MetadataException, "You have not specified the database collection" if db_collection_param.nil? or db_collection_param.empty?
      db_collection = @db[db_collection_param]
      db_key = generate_key(schedule_id,execution_id)
      record = db_collection.find({"_id" => db_key}).limit(1)
      if (record.count == 0)
        hash_for_storage = {"_id" => db_key, "created_at" => Time.now.utc,"updated_at" => Time.now.utc, "metadata" => @hash }
        db_collection.insert(hash_for_storage)
      else
        hash_for_storage = record.first
        hash_for_storage["updated_at"]  = Time.now.utc
        hash_for_storage["metadata"]    = @hash
        db_collection.update({"_id" => db_key},hash_for_storage)
      end
    end

    # This method is called at metadata initialization time and it will load metadata (global) from metadata storage
    def load_global_hash(schedule_id,options = {})
      db_collection_param = options["db_collection"]
      raise MetadataException, "You have not specified the database collection" if db_collection_param.nil? or db_collection_param.empty?
      db_collection = @db[db_collection_param]
      db_key = schedule_id
      response = db_collection.find(:_id => db_key)
      raise MetadataException, "The response from database has returned more than one record. Critical error please contact administrator" if response.count > 1
      @hash = response.first["global_metadata"]
    end


    # This method is called at metadata initialization time and it will load metadata (global) from metadata storage
    def load_hash(schedule_id,execution_id,options={})
      db_collection_param = options["db_collection"]
      raise MetadataException, "You have not specified the database collection" if db_collection_param.nil? or db_collection_param.empty?
      db_collection = @db[db_collection_param]
      db_key = generate_key(schedule_id,execution_id)
      response = db_collection.find(:_id => db_key)
      raise MetadataException, "The response from database has returned more than one record. Critical error please contact administrator" if response.count > 1
      @hash = response.first["metadata"]
    end

    def check_configuration_mandatory_parameters(mandatory_parameters)
      $log.info "Starting mandatory fields check"
      missing_fields = {}
      mandatory_parameters.each_pair do |key,value|
        if (@hash["configuration"].has_key?(key))
          value.each do |field|
            if (!@hash["configuration"][key].has_key?(field))
              missing_fields[key] = [] if missing_fields[key].nil?
              missing_fields[key] << field
            end
          end
        else
          raise MissingParametersException, "The connector part #{@TYPE} is missing mandatory section of configuration. Missing section: #{key}"
        end
      end
      if (missing_fields.keys.count > 0)
        messages = []
        missing_fields.each_pair {|k,values| messages << "#{k} -> (#{values.join(",")})"}
        raise MissingParametersException, "The connector part #{@TYPE} is missing mandatory section of configuration. #{messages.join(" , ")}"
      end
      $log.info "Mandatory field check has finished"
    end

    def get_configuration_by_type(type)
      if (@hash["configuration"].has_key?(type))
        @hash["configuration"][type]
      end
    end

    def get_configuration_by_type_and_key(type,key)
      if (@hash["configuration"].include?(type))
        if (@hash["configuration"][type].has_key?(key))
          @hash["configuration"][type][key]
        end
      end
    end


    def merge_default_configuration(default_configuration)
      @hash["configuration"].each_pair do |k,v|
        if (default_configuration.has_key?(k))
          configuration_level = v
          default_level = default_configuration[k]
          compare_level_of_hash(configuration_level,default_level)
        end
      end
    end


    def add_default_entities(default_entities)
      if (!@hash.include?("entities"))
        @hash["entities"] = {}
      end
      default_entities.each do |entity|
        if (!@hash["entities"].include?(entity))
          @hash["entities"][entity] = {"id" => entity}
        end
      end

    end


    def get_entity(entity)
      @hash["entities"][entity]
    end

    def add_entity(entity)
      if (!@hash.include?("entities"))
        @hash["entities"] = {}
      end
      @hash["entities"][entity["id"]] = entity
    end

    def list_entities
      @hash["entities"].keys
    end


    def print_hash
      pp @hash
    end


    private

    # Connection to MongoDB
    def connect(options = {})
      host = options["database_host"] || "localhost"
      use_ssl = options["use_ssl"] || false
      username = options["username"] || nil
      password = options["password"] || nil
      db_name  = options["db_name"]

      begin
        @client = MongoClient.new(host = host,user_ssl = use_ssl,username = username,password = password)
        @db     = @client[db_name]
      rescue => e
        raise MetadataException, e.message
      end
    end

    # Generation of key, which is used to store metadata in metadata storage
    def generate_key(schedule_id, execution_id,options = {})
      raise MetadataExpcetion, "The schedule_id or execution_id are null, cannot generate database key" if schedule_id.nil? or execution_id.nil?
      "#{schedule_id}-#{execution_id}"
    end


    # This method is merging the client configuration with default compoment configuration
    def compare_level_of_hash(source,target)
      target.each_pair do |k,v|
        if (v.instance_of?(Array))
          if (!source.has_key?(k))
            source[k] = v
          else
            if (source[k].instance_of?(Array))
              source[k] = (source[k] + v).uniq
            end
          end
        elsif (v.instance_of?(Hash))
          if (source.has_key?(k))
            if (source[k].instance_of?(Hash))
              compare_level_of_hash(source[k],v)
            end
          else
            source[k] = v
          end
        else
          if (!source.has_key?(k))
            source[k] = v
          end
        end
      end
    end

  end

end