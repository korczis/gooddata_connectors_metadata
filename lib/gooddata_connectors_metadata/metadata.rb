# encoding: UTF-8

module GoodData
  module Connectors
    module Metadata

      class ::Hash
        def deep_merge(second)
          merger = proc { |key, v1, v2| Hash === v1 && Hash === v2 ? v1.merge(v2, &merger) : [:undefined, nil, :nil].include?(v2) ? v1 : v2 }
          self.merge(second, &merger)
        end
      end



      class Metadata

        CONFIGURATION_PATH = "metadata/entities.json"
        SOURCE_METADATA_PATH = "metadata/source.json"

        attr_accessor :bds_client,:entities,:global_configuration

        def initialize(options = {})
          @hash = {}
          load_configuration(options)
          @bds_client = connect(options)
          @entities = Entities.new
          @global_configuration = @bds_client.load_configuration
          # Initialize time of load
          Runtime.fill
          # Lets load configuration files
        end


        def set_source_context(source,options = {},source_object = nil)
          @bds_client.context.data_source = source
          if (@global_configuration.include?("downloaders"))
            downloader_settings = @global_configuration["downloaders"].find{|settings| settings["id"] == source}
            if (!downloader_settings.nil?)
              if (downloader_settings.include?("entities"))
                downloader_settings["entities"].each do |entity_name|
                  if (!@global_configuration["entities"].include?(entity_name))
                    raise MetadataException,"The downloader settings is containing entity, which is not in global entity list #{entity_name}"
                  end
                  # Load of the last timestamp
                  @bds_client.context.entity = entity_name
                  start_date = @bds_client.last_business_date(entity_name)
                  main_entity = load_entity_configuration(entity_name,source_object)
                  main_entity.store_runtime_param("start_date",start_date)
                  if (downloader_settings.include?("customers"))
                    downloader_settings["customers"].each do |customer|
                      start_date = @bds_client.last_business_date(entity_name,customer)
                      load_custom_entity_configuration(entity_name,customer,source_object)
                      main_entity.store_runtime_param("start_date",start_date,customer)
                    end
                  end
                  # Let check if there are any connected entities
                  if (@global_configuration["entities"][entity_name]["global"].include?("connected_to"))
                    @global_configuration["entities"][entity_name]["global"]["connected_to"].each do |connected_entity|
                      entity = load_entity_configuration(connected_entity,source_object,entity_name)
                      entity.store_runtime_param("start_date",main_entity.get_runtime_param("start_date"))
                      if (downloader_settings.include?("customers"))
                        downloader_settings["customers"].each do |customer|
                          load_custom_entity_configuration(connected_entity,customer,source_object)
                          entity.store_runtime_param("start_date",main_entity.get_runtime_param("start_date",customer),customer)
                        end
                      end
                    end
                  end
                end
              else
                raise MetadataException,"There are no entities settings in config file. ( Missing entities element in element.json)"
              end
            else
              raise MetadataException,"There is not downloader with id: #{source}"
            end
          end
        end

        def load_entity_configuration(entity_name,source_object,parent_entity = nil)
          entity_settings = @global_configuration["entities"][entity_name]["global"]
          response = @bds_client.get_entity_metadata(entity_name)
          metadata = response[:metadata]
          entity_settings["custom"] = {} if !entity_settings.include?("custom")
          # If there is no field setting in metadata, lets load the fields in downloder
          entity = nil
          if (!entity_settings.include?("fields") or entity_settings["fields"].count == 0 )
            entity_settings["custom"]["automatic"] = true
          else
            entity_settings["custom"]["automatic"] = false
          end
          if (!metadata.nil?)
            $log.info "Metadata file found on storage for entity #{entity_name}."
            entity_settings.merge!({"dependent_on" => parent_entity}) unless parent_entity.nil?
            entity_from_metadata = Entity.new("hash" => metadata)
            entity_from_setting = Entity.new("hash" => entity_settings)
            entity_from_metadata.merge!(entity_from_setting)
            entity = entity_from_metadata
            entity.store_runtime_param("metadata_date",response[:time_element])
            entity.store_runtime_param("metadata_file","#{response[:time_element][:timestamp]}_metadata.json")
          else
            # The metadata could not be found on S3, we are creating new default metadata
            $log.info "Metadata file not found on storage for entity #{entity_name}. Creating new metadata."
            if (!source_object.nil?)
              default_settings = {}
              if (source_object.define_default_entities.include?(entity_name))
                default_settings = source_object.define_default_entities[entity_name]
              end
              merged_settings = entity_settings.merge(default_settings){|key,oldval,newval| oldval}
              merged_settings.merge!({"id" => entity_name,"name" => entity_name})
              merged_settings.merge!({"dependent_on" => parent_entity}) unless parent_entity.nil?
              entity = Entity.new("hash" => merged_settings)
            end
          end
          @entities << entity
          entity
        end

        def load_custom_entity_configuration(entity_name, customer,source_object)
          customer_settings = {}
          if (@global_configuration["entities"][entity_name].include?(customer))
            customer_settings = @global_configuration["entities"][entity_name][customer]
          end
          # Currently we are using only fields configuration from customer settings
          raise MetadataException, "You are trying add custom entity, which don't heva global settings. Entity Name: #{entity_name}" if !@entities.include?(entity_name)
          entity = @entities[entity_name]
          response = @bds_client.get_entity_metadata(entity_name,customer)
          metadata = response[:metadata]
          if (!metadata.nil?)
            $log.info "Metadata for customer #{customer} found on storage."
            metadata["fields"].each do |field|
              entity.add_field(field,customer)
            end
            if (customer_settings.include?("fields"))
              customer_settings["fields"].each do |settings_field|
                if (!entity.field_exist?(settings_field,customer))
                  entity.add_field({"id" => settings_field,"type" => "string-255"},customer)
                end
              end
            end
            entity.get_enabled_customer_fields_objects(customer).each do |field|
                if (!customer_settings.include?("fields") or !customer_settings["fields"].include?(field.id))
                  field.disable("Removed from customer settings")
                end
            end
            entity.store_runtime_param("metadata_date",response[:time_element],customer)
            entity.store_runtime_param("metadata_file","#{response[:time_element][:timestamp]}_metadata.json")
          else
            # The metadata could not be found on S3, we are creating new default metadata
            if (!source_object.nil?)
              customer_settings = {}
              if (@global_configuration["entities"][entity_name].include?(customer))
                customer_settings = @global_configuration["entities"][entity_name][customer]
              end
              if (customer_settings.include?("fields"))
                $log.info "Metadata for #{customer} not found on storage. Creating."
                customer_settings["fields"].each do |settings_field|
                  entity.add_field({"id" => settings_field,"type" => "string-255"},customer)
                end
              end
            end
          end
          entity
        end


        def load_configuration(options)
          @hash['configuration'] = {}
          @hash['configuration']['global'] = options
          @hash['configuration'] = @hash['configuration'].deep_merge(Configuration.load_from_schedule(options))
        end

        def save
          @entities.each do |entity|
           save_entity(entity)
          end
        end

        def save_entity(entity)
          downloader_settings = @global_configuration["downloaders"].find{|settings| settings["id"] == @bds_client.context.data_source}
          File.open("metadata/#{Runtime.now_timestamp}_#{entity.id}_metadata.json","w") do |f|
            f.write(JSON.pretty_generate(entity.to_hash))
          end
          full_path = @bds_client.context.construct_full_metadata_path(entity.id,"#{Runtime.now_timestamp}_metadata.json",Runtime.now)
          result = @bds_client.store(full_path,{:file => "metadata/#{Runtime.now_timestamp}_#{entity.id}_metadata.json"})
          raise MetadataException, "The file #{full_path} was not saved on S3" if result[:status] == :failed
          entity.store_runtime_param("metadata_file","#{Runtime.now_timestamp}_metadata.json")
          entity.store_runtime_param("metadata_date",{:year => TimeHelper.year(Runtime.now) ,:month => TimeHelper.month(Runtime.now),:day => TimeHelper.day(Runtime.now),:timestamp => Runtime.now_timestamp })
          if (!downloader_settings.nil? and downloader_settings.include?("customers"))
            downloader_settings["customers"].each do |customer|
              if (entity.customer_have_fields?(customer))
                save_customer_metadata(entity,customer)
              end
            end
          end
        end


        def save_customer_metadata(entity,customer)
          local_file_path = "metadata/#{customer}/#{Runtime.now_timestamp}_#{entity.id}_metadata.json"
          FileUtils.mkdir_p(File.dirname(local_file_path))
          File.open(local_file_path,"w") do |f|
            f.write(JSON.pretty_generate(entity.to_hash(customer)))
          end
          full_path = @bds_client.context.construct_full_metadata_path(entity.id,"#{customer}/#{Runtime.now_timestamp}_metadata.json",Runtime.now)
          result = @bds_client.store(full_path,{:file => local_file_path})
          raise MetadataException, "We file #{full_path} was not saved on S3. Result: #{result[:reason]}" if result[:status] == :failed
          entity.store_runtime_param("metadata_file","#{Runtime.now_timestamp}_metadata.json",customer)
          entity.store_runtime_param("metadata_date",
                                      {:year => TimeHelper.year(Runtime.now),
                                       :month => TimeHelper.month(Runtime.now),
                                       :day => TimeHelper.day(Runtime.now),
                                       :timestamp => Runtime.now_timestamp
                                      },customer)
        end

        def save_data(entity,customer = nil)
          file = entity.get_runtime_param("source_filename",customer)
          remote_path = ""
          if (customer.nil?)
            remote_path = "#{Metadata::Runtime.now_timestamp}_data.csv"
          else
            remote_path = "#{customer}/#{Runtime.now_timestamp}_data.csv"
          end
          @bds_client.store_data(entity.id,file,remote_path,Runtime.now,entity.runtime)
        end

        # This method should be called from client, anytime the metadata (global) are changed
        # def store_global_hash(options = {})
        #   db_collection_param = options['db_collection'] || 'default'
        #   fail MetadataException, 'You have not specified the database collection' if db_collection_param.nil? || db_collection_param.empty?
        #   db_collection = @db[db_collection_param]
        #   fail MetadataExpcetion, 'The schedule_id is null, cannot generate database key' if $SCHEDULE_ID.nil?
        #   db_key = $SCHEDULE_ID
        #   record = db_collection.find('_id' => db_key).limit(1)
        #   if record.count == 0
        #     metadata_hash = {
        #       'entities' => @entities.to_hash,
        #       'runtime' => Runtime.to_hash
        #     }
        #     hash_for_storage = { '_id' => db_key, 'created_at' => Time.now.utc, 'updated_at' => Time.now.utc, 'metadata' => metadata_hash }
        #     db_collection.insert(hash_for_storage)
        #   else
        #     hash_for_storage = record.first
        #     hash_for_storage['updated_at'] = Time.now.utc
        #     hash_for_storage['metadata'] = {
        #       'entities' => @entities.to_hash,
        #       'runtime' => Runtime.to_hash
        #     }
        #     db_collection.update({ '_id' => db_key }, hash_for_storage)
        #   end
        # end

        def get_context_entities_ids
          downloader_settings = @global_configuration["downloaders"].find{|settings| settings["id"] == @bds_client.context.data_source}
          if (!downloader_settings.nil? and downloader_settings.include?("entities"))
            entities_with_dependencies = get_entity_list_with_dependencies
            downloader_settings["entities"].each do |entity_id|
              entities_with_dependencies.delete_if do |k,v|
                !downloader_settings["entities"].include?(k)
              end

            end
            entities_with_dependencies
          else
            {}
          end
        end


        def get_context_customers
          downloader_settings = @global_configuration["downloaders"].find{|settings| settings["id"] == @bds_client.context.data_source}
          if (!downloader_settings.nil? and downloader_settings.include?("customers"))
            downloader_settings["customers"]
          else
            []
          end
        end

        def load_fields_from_source?(entity_id)
          !@global_configuration["entities"][entity_id]["global"].include?("fields") or @global_configuration["entities"][entity_id]["global"]["fields"].empty?
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

        def context
          @bds_client.context
        end


        def timestamp_to_time(timestamp)
          @bds_client.timestamp_to_time(timestamp)
        end


        def print_hash
          pp @hash
        end

        private

        # Connection to MongoDB
        def connect(options = {})
          bds_access_key = get_configuration_by_type_and_key("global","bds_access_key")
          bds_secret_key = get_configuration_by_type_and_key("global","bds_secret_key")
          account_id = get_configuration_by_type_and_key("global","account_id")
          token = get_configuration_by_type_and_key("global","token")
          bucket = get_configuration_by_type_and_key("global","bds_bucket")
          default_folder = get_configuration_by_type_and_key("global","bds_folder")
          connection = S3Bds.new(
              {
                  :key => bds_access_key,
                  :secret => bds_secret_key,
                  :account_id => account_id,
                  :token => token,
                  :bucket => bucket,
                  :default_folder => default_folder
              }
          )
          connection
        end

        # Generation of key, which is used to store metadata in metadata storage
        def generate_key(schedule_id, execution_id, options = {})
          fail MetadataExpcetion, 'The schedule_id or execution_id are null, cannot generate database key' if schedule_id.nil? || execution_id.nil?
          "#{schedule_id}-#{execution_id}"
        end

        # This method is merging the client configuration with default compoment configuration
        def compare_level_of_hash(source, target)
          pp source
          pp target
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
