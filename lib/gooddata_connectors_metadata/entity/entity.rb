# encoding: UTF-8

module GoodData
  module Connectors
    module Metadata
      class Entity
        attr_accessor :id, :name, :custom, :fields, :type, :runtime, :dependent_on, :validations

        # {
        #    "id" => "id",
        #    "name" => "name",
        #    "type" => "type",
        #    "custom" => {
        #        "test1" => "value"
        #        etc
        #    }
        # }

        # Requesting id, name, type (optional custom)
        # or
        # hash
        def initialize(args = {})
          @fields = {}
          @validations = {}
          if !args['hash'].nil?
            from_hash(args['hash'])
          elsif !args['id'].nil?
            @id = args['id']
            @name = args['name'] || args['id']
            @history = args['history'] || {}
            @custom = args['custom'] unless (args['custom'].nil?)
            @enabled = args['enabled'] || true
            @type = args['type'] || 'input|output'
            @runtime = args['runtime'] || {}
            @dependent_on = args['dependent_on'] || nil
            unless args['fields'].nil?
              args['fields'].each do |field|
                add_field(field)
              end
            end
          else
            fail EntityException, 'Missing mandatory parameters when creating entity, mandatory fields are id,name,type or hash'
          end
        end

        def ==(entity)
          @id == entity.id
        end

        def to_hash
          {
            'id' => @id,
            'name' => @name,
            'custom' => @custom,
            'fields' => @fields.map { |k, v| v.to_hash },
            'history' => @history,
            'enabled' => @enabled,
            'type' => @type,
            'runtime' => @runtime,
            'dependent_on' => @dependent_on
          }
        end

        def from_hash(hash)
          @id = hash['id']
          @name = hash['name']
          @custom = hash['custom'] unless hash['custom'].nil?
          @history = hash['history'] || {}
          @enabled = hash['enabled'] || true
          @type = hash['type'] || 'input|output'
          @runtime = hash['runtime'] || {}
          @dependent_on = hash['dependent_on'] || nil
          unless hash['fields'].nil?
            hash['fields'].each do |field_element|
              # Lets process the fields from the configuration file
              # The fields could be in form of Array or in form of HASH with type
              # Array  - ["Name","Id","Stage"]
              # Hash - [
              #  {"id" => "Name","type" => "string-18"}
              #  {"id" => "Id","type" => "string-18"}
              #  {"id" => "Stage","type" => "string-10"}
              #  ]
              if field_element.instance_of?(String)
                field = Field.new('id' => field_element)
              elsif field_element.instance_of?(Hash) || field_element.instance_of?(BSON::OrderedHash)
                field = Field.new('hash' => field_element)
              else
                fail MetadataException, 'Wrong parsing of field'
              end
              @fields[field.id] = field
            end
          end
        end

        def field_exist?(id)
          @fields.include?(id)
        end

        def get_field(id)
          @fields[id]
        end

        def delete_field(id, reason = '')
          @fields[id].disable
          @fields[id].custom['disable_reason'] = reason
        end

        def get_ids
          fields.map { |f| f.id }
        end

        def add_field(input)
          if input.instance_of?(String)
            field = Field.new('id' => input)
          elsif input.instance_of?(Hash)
            field = Field.new('hash' => input)
          elsif input.instance_of?(Field)
            field = input
          end
          @fields[field.id] = field
        end

        def disable(reason = '')
          @enabled = false
          @custom['disable_reason'] = reason
        end

        def disabled?
          !@enabled
        end

        def input?
          @type =~ /input/
        end

        def output?
          @type =~ /output/
        end

        def get_enabled_fields
          @fields.values.find_all { |v| !v.disabled? }.map { |v| v.id }
        end

        def add_validation(key, type, validation)
          if !@validations.include?(key)
            @validations[key] = {}
          end
          if !@validations[key].include?(type)
            @validations[key][type] = nil
          end
          @validations[key][type] = validation
        end

        def get_validation_by_type(key, type)
          if !@validations[key].nil? && @validations[key].include?(type)
            @validations[key][type]
          end
        end

        def merge!(entity, enable_add = true)
          @custom.merge! entity.custom unless entity.custom.nil?
          @name = entity.name unless entity.name.nil?
          @dependent_on = entity.dependent_on unless entity.dependent_on.nil?
          @runtime.merge!(entity.runtime) unless entity.runtime.nil?
          fields_to_disable = @fields.values - entity.fields.values
          fields_to_add = entity.fields.values - @fields.values
          fields_to_merge = @fields.values & entity.fields.values

          if !entity.fields.values.empty?
            fields_to_disable.each do |field|
              field.disable('from merge')
            end
          end

          if enable_add
            fields_to_add.each do |field|
              field.custom['synchronized'] = false
              add_field(field)
            end
          end

          fields_to_merge.each do |field|
            field.merge!(entity.get_field(field.id), enable_add)
          end
        end

        # This method with compare two entities.(It is done from perpective of first entity - entity on which the diff command is called)
        # It is currently comparing
        # Name
        # Disabled
        # Fields
        # IT will return the changes in hash
        def diff(entity)
          changes = {}
          changes['name'] = {'source' => @name, 'target' => entity.name} if @name.downcase != entity.name.downcase
          changes['disabled'] = {'source' => disabled?, 'target' => entity.disabled?} if disabled? != entity.disabled?
          changes['type'] = {'source' => @type, 'target' => entity.type} if (@type.split('|') & entity.type.split('|')).count != @type.split('|').count
          changes['dependent_on'] = {'source' => @dependent_on, 'target' => entity.dependent_on} if @dependent_on != entity.dependent_on
          changes['fields'] = {}
          changes['fields']['only_in_source'] = []
          changes['fields']['only_in_target'] = []
          changes['fields']['changed'] = []
          fields_only_in_source = @fields.values - entity.fields.values
          fields_only_in_target = entity.fields.values - @fields.values
          fields_in_both_collections = @fields.values & entity.fields.values

          fields_only_in_source.each do |field|
            changes['fields']['only_in_source'] << field
          end

          fields_only_in_target.each do |field|
            changes['fields']['only_in_target'] << field
          end

          fields_in_both_collections.each do |field|
            target_field = entity.get_field(field.id)
            hash = {}
            hash['name'] = {'source' => field.name, 'target' => target_field.name} if field.name.downcase != target_field.name.downcase
            hash['type'] = {'source' => field.type, 'target' => target_field.type} if field.type != target_field.type
            hash['disabled'] = {'source' => field.disabled?, 'target' => target_field.disabled?} if field.disabled? != target_field.disabled?
            hash['field'] = field if !hash.empty?
            changes['fields']['changed'] << hash if !hash.empty?
          end
          changes
        end

        # @param [Array] folders List of folders where we should look for validations
        # @param [String] type Type of component which requested validation listing
        def generate_validations(folders, type)
          pp folders
          folders.each do |folder|
            list_of_file = Dir["#{folder}/*.erb"]
            @templates = {}
            list_of_file.each do |file|
              key = File.basename(file).split('.').first
              # File name should be in this format type_entity.query_language.erb
              decommission = key.split('_')
              if decommission.count == 2
                if decommission[1] == @id
                  validation = Validation.new(type, file)
                  add_validation(decommission[0], type, validation)
                end
              else
                validation = Validation.new(type, file)
                add_validation(decommission[0], type, validation)
              end
            end
          end
        end
      end
    end
  end
end
