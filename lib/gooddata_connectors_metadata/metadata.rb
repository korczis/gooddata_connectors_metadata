# encoding: UTF-8

require 'mongo'

module GoodData
  module Connectors
    module Metadata
      class Metadata
        include ::Mongo

        def initialize(options = {})
          connect(options)
          @global_hash = {}
          @hash = {}
          @entities = Entities.new
          # Lets load configuration files
          @hash['configuration'] = {}
          if options['configuration_folder']
            @hash['configuration'].merge!(Configuration.load_from_files(options['configuration_folder']))
          end
          @hash['configuration'].merge!(Configuration.load_from_schedule(options))
          @hash['configuration']['global'] = options
          pp @hash['configuration']
        end

        # This method should be called from client, anytime the metadata (global) are changed
        def store_global_hash(options = {})
          db_collection_param = options['db_collection'] || 'default'
          fail MetadataException, 'You have not specified the database collection' if db_collection_param.nil? || db_collection_param.empty?
          db_collection = @db[db_collection_param]
          fail MetadataExpcetion, 'The schedule_id is null, cannot generate database key' if $SCHEDULE_ID.nil?
          db_key = $SCHEDULE_ID
          record = db_collection.find('_id' => db_key).limit(1)
          if record.count == 0
            metadata_hash = {
              'entities' => @entities.to_hash,
              'runtime' => Runtime.to_hash
            }
            hash_for_storage = { '_id' => db_key, 'created_at' => Time.now.utc, 'updated_at' => Time.now.utc, 'metadata' => metadata_hash }
            db_collection.insert(hash_for_storage)
          else
            hash_for_storage = record.first
            hash_for_storage['updated_at'] = Time.now.utc
            hash_for_storage['metadata'] = {
              'entities' => @entities.to_hash,
              'runtime' => Runtime.to_hash
            }
            db_collection.update({ '_id' => db_key }, hash_for_storage)
          end
        end

        # # This method should be called from client, anytime the metadata (per execution) are changed
        # def store_hash(schedule_id, execution_id, options = {})
        #   db_collection_param = options["db_collection"] || "default"
        #   fail MetadataException, "You have not specified the database collection" if db_collection_param.nil? || db_collection_param.empty?
        #   db_collection = @db[db_collection_param]
        #   db_key = generate_key(schedule_id, execution_id)
        #   record = db_collection.find({"_id" => db_key}).limit(1)
        #   if (record.count == 0)
        #     hash_for_storage = {"_id" => db_key, "created_at" => Time.now.utc, "updated_at" => Time.now.utc, "metadata" => @hash}
        #     db_collection.insert(hash_for_storage)
        #   else
        #     hash_for_storage = record.first
        #     hash_for_storage["updated_at"] = Time.now.utc
        #     hash_for_storage["metadata"] = @hash
        #     db_collection.update({ "_id" => db_key }, hash_for_storage)
        #   end
        # end

        # This method is called at metadata initialization time and it will load metadata (global) from metadata storage
        def load_global_hash(options = {})
          db_collection_param = options['db_collection'] || 'default'
          fail MetadataException, 'You have not specified the database collection' if db_collection_param.nil? || db_collection_param.empty?
          db_collection = @db[db_collection_param]
          db_key = $SCHEDULE_ID
          response = db_collection.find('_id' => db_key).limit(1)
          fail MetadataException, 'The response from database has returned more than one record. Critical error please contact administrator' if response.count > 1
          if response.count == 1
            hash_for_storage = response.first
            @global_hash = hash_for_storage['metadata'] || {}
            if !@global_hash.empty?
              @entities = Entities.new('hash' => @global_hash['entities'])
              Runtime.fill(@global_hash['runtime'] || {})
            else
              Runtime.fill({})
            end
          elsif response.count == 0
            Runtime.fill({})
          end

          response = db_collection.find('_id' => db_key).limit(1)
          value = response.first
          if value
            unless value.include?('history')
              db_collection.update({ '_id' => db_key }, { 'history' => [] })
            end
          end
          response = db_collection.find('_id' => db_key).limit(1)
          hash_for_storage = response.first
          if hash_for_storage && hash_for_storage.include?('metadata') && hash_for_storage['metadata'].include?('entities')
            db_collection.update({ '_id' => db_key },
                                 { '$push' => {
                                   'history' => {
                                     'load_id' => Runtime.get_load_id,
                                     'date' => Time.now.utc,
                                     'metadata' => {
                                       'entities' => hash_for_storage['metadata']['entities']
                                     }
                                   }
                                  }
                                 }
            )

          end
          Runtime.set_load_id(Runtime.get_load_id + 1)
        end

        # # This method is called at metadata initialization time and it will load metadata (global) from metadata storage
        # def load_hash(schedule_id, execution_id, options={})
        #   db_collection_param = options["db_collection"] || "default"
        #   fail MetadataException, "You have not specified the database collection" if db_collection_param.nil? || db_collection_param.empty?
        #   db_collection = @db[db_collection_param]
        #   db_key = generate_key(schedule_id, execution_id)
        #   response = db_collection.find(:_id => db_key)
        #   fail MetadataException, "The response from database has returned more than one record. Critical error please contact administrator" if response.count > 1
        #   @hash = response.first["metadata"]
        # end

        def check_configuration_mandatory_parameters(mandatory_parameters)
          $log.info 'Starting mandatory fields check'
          missing_fields = {}
          mandatory_parameters.each_pair do |key, value|
            if @hash['configuration'].key?(key)
              value.each do |field|
                unless @hash['configuration'][key].key?(field)
                  missing_fields[key] = [] if missing_fields[key].nil?
                  missing_fields[key] << field
                end
              end
            else
              fail MissingParametersException, "The connector part #{@TYPE} is missing mandatory section of configuration. Missing section: #{key}"
            end
          end
          if missing_fields.keys.count > 0
            messages = []
            missing_fields.each_pair { |k, values| messages << "#{k} -> (#{values.join(',')})" }
            fail MissingParametersException, "The connector part #{@TYPE} is missing mandatory section of configuration. #{messages.join(' , ')}"
          end
          $log.info 'Mandatory field check has finished'
        end

        def get_configuration_by_type(type)
          @hash['configuration'][type] if @hash['configuration'].key?(type)
        end

        def get_configuration_by_type_and_key(type, key)
          if @hash['configuration'].include?(type)
            if @hash['configuration'][type].key?(key)
              @hash['configuration'][type][key]
            end
          end
        end

        def merge_default_configuration(default_configuration)
          @hash['configuration'].each_pair do |k, v|
            if default_configuration.key?(k)
              configuration_level = v
              default_level = default_configuration[k]
              compare_level_of_hash(configuration_level, default_level)
            end
          end
        end

        # Add entities from configuration file to global_entities storage (merging)
        # Configuration is top most priority when merging
        def add_entities
          if @hash['configuration'].include?('entities')
            @hash['configuration']['entities'].each_pair do |entity_name, entity_hash|
              if entity_hash.include?('fields') && !entity_hash['fields'].empty?
                if entity_hash.include?('custom')
                  entity_hash['custom'].merge!('load_fields_from_source_system' => false)
                else
                  entity_hash['custom'] = { 'load_fields_from_source_system' => false }
                end
              else
                if entity_hash.include?('custom')
                  entity_hash['custom'].merge!('load_fields_from_source_system' => true)
                else
                  entity_hash['custom'] = { 'load_fields_from_source_system' => true }
                end
              end
              entity_hash.merge!('id' => entity_name) unless entity_hash.include?('id')

              if @entities.include?(entity_name)
                entity = @entities[entity_name]
                config_entity = Entity.new('hash' => entity_hash)
                entity.merge! config_entity
              else
                @entities << Entity.new('hash' => entity_hash)
              end
            end

            # We have entities section in cofiguration file, so we disable all other entities.
            # In this case, user need to specificly name the entity which he want to download
            entities_to_disable = @entities.get_entity_names - @hash['configuration']['entities'].map { |k, v| k }
            entities_to_disable.each do |entity_to_disable|
              @entities[entity_to_disable].disable('Override by user config') unless @entities[entity_to_disable].disabled?
            end
          end
        end

        # Lets add default entities, wchich should be donwloaded by downloader
        # THis entities are added to entities list in metadata storage
        def add_default_entities(default_entities)
          # Adding default entities only in case that user has not specified the entities by himself
          unless @hash['configuration'].include?('entities')

            default_entities.each_pair do |entity_name, entity_hash|
              if entity_hash.include?('fields') && !entity_hash['fields'].empty?
                if entity_hash.include?('custom')
                  entity_hash['custom'].merge!('load_fields_from_source_system' => false)
                else
                  entity_hash['custom'] = { 'load_fields_from_source_system' => false }
                end
              else
                if entity_hash.include?('custom')
                  entity_hash['custom'].merge!('load_fields_from_source_system' => true)
                else
                  entity_hash['custom'] = { 'load_fields_from_source_system' => true }
                end
              end
              entity_hash.merge!('id' => entity_name) unless entity_hash.include?('id')

              if @entities.include?(entity_name)
                entity = @entities[entity_name]
                config_entity = Entity.new('hash' => entity_hash)
                entity.merge! config_entity
              else
                @entities << Entity.new(entity_hash)
              end
            end
            # We have entities section in cofiguration file, so we disable all other entities.
            # In this case, user need to specificly name the entity which he want to download
            entities_to_disable = @entities.get_entity_names - default_entities.map { |k, v| k }
            entities_to_disable.each do |entity_to_disable|
              @entities[entity_to_disable].disable('Change in global default entity settings') unless @entities[entity_to_disable].disabled?
            end
          end
        end

        def get_entity(entity)
          @entities[entity]
        end

        def add_entity(entity)
          @entities << entity
        end

        def list_entities # rubocop:disable TrivialAccessors
          @entities
        end

        def get_entity_list_with_dependencies
          @entities.get_entity_list_with_dependencies
        end

        def now # rubocop:disable TrivialAccessors
          @now
        end

        def print_hash
          pp @hash
        end

        private

        # Connection to MongoDB
        def connect(options = {})
          host = options['db_host'] || 'localhost'
          use_ssl = options['use_ssl'] || false
          username = options['db_username'] || nil
          password = options['db_password'] || nil
          db_name = options['db_name']

          begin
            # TODO: This looks strange, should it be :host = host, :user_ssl = use_ssl
            @client = MongoClient.new(host, ssl: use_ssl)
            @db = @client[db_name]
            _auth = @db.authenticate(username, password)
          rescue => e
            raise MetadataException, e.message
          end
        end

        # Generation of key, which is used to store metadata in metadata storage
        def generate_key(schedule_id, execution_id, options = {})
          fail MetadataExpcetion, 'The schedule_id or execution_id are null, cannot generate database key' if schedule_id.nil? || execution_id.nil?
          "#{schedule_id}-#{execution_id}"
        end

        # This method is merging the client configuration with default compoment configuration
        def compare_level_of_hash(source, target)
          target.each_pair do |k, v|
            if v.instance_of?(Array)
              if !source.key?(k)
                source[k] = v
              else
                if source[k].instance_of?(Array)
                  source[k] = (source[k] + v).uniq
                end
              end
            elsif v.instance_of?(Hash)
              if source.key?(k)
                if source[k].instance_of?(Hash)
                  compare_level_of_hash(source[k], v)
                end
              else
                source[k] = v
              end
            else
              source[k] = v unless source.key?(k)
            end
          end
        end
      end
    end
  end
end
