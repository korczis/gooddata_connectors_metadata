# encoding: UTF-8

module GoodData
  module Connectors
    module Metadata
      class Entity
        attr_accessor :id, :name, :custom, :fields, :type, :runtime, :dependent_on, :validations,:customer

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
          @customer_fields = {}
          @validations = {}
          @runtime = {}
          if args['hash']
            from_hash(args['hash'])
          elsif args['id']
            from_hash(args)
          else
            fail EntityException, 'Missing mandatory parameters when creating entity, mandatory fields are id,name,type or hash'
          end
        end

        def ==(other)
          @id == other.id
        end

        def to_hash(customer = nil)
          if (customer.nil?)
            {
              'id' => @id,
              'name' => @name,
              'custom' => @custom,
              'fields' => @fields.map { |k, v| v.to_hash },
              'enabled' => @enabled,
              'type' => @type,
              'dependent_on' => @dependent_on,
            }
          else
            {
                'id' => @id,
                'name' => @name,
                'fields' => get_customer_fields_objects(customer).map{ |v| v.to_hash}
            }
          end
        end

        def from_hash(hash)
          @id = hash['id']
          @name = hash['name']
          @custom = hash['custom'] unless hash['custom'].nil?
          @enabled = hash['enabled'] || true
          @type = hash['type'] || 'input|output'
          @dependent_on = hash['dependent_on'] || nil
          unless hash['fields'].nil?
            hash['fields'].each do |field_element|
              add_field(field_element)
            end
          end

        end

        def field_exist?(id,customer = nil)
          if (!customer.nil?)
            @fields.include?(id) || @customer_fields[customer].include?(id)
          else
            @fields.include?(id)
          end
        end

        def get_field(id,customer = nil)
          if (!customer.nil?)
            @fields[id] if (@fields.include?(id))
            @customer_fields[customer][id] if (@customer_fields[customer].include?(id))
          else
            @fields[id]
          end
        end

        def delete_field(id, reason = '')
          @fields[id].disable
          @fields[id].custom['disable_reason'] = reason
        end

        def get_ids(customer = nil)
          fields.keys  + @customer_fields[customer].keys
        end

        def store_runtime_param(key,value,customer = nil)
          if (customer.nil?)
            @runtime[key] = value
          else
            @runtime[customer] = {} if (!@runtime.include?(customer))
            @runtime[customer][key] = value
          end
        end

        def get_runtime_param(key,customer = nil)
          if (customer.nil?)
            @runtime[key]
          else
            @runtime[customer][key]
          end
        end


        def add_field(input,customer = nil)
          # Lets process the fields from the configuration file
          # The fields could be in form of Array or in form of HASH with type
          # Array  - ["Name","Id","Stage"]
          # Hash - [
          #  {"id" => "Name","type" => "string-18"}
          #  {"id" => "Id","type" => "string-18"}
          #  {"id" => "Stage","type" => "string-10"}
          #  ]
          field = nil
          if input.instance_of?(String)
            field = GoodData::Connectors::Metadata::Field.new({'id' => input,'order' => get_new_order_id(customer)})
          elsif input.instance_of?(Hash)
            if (!input.include?("order") or input["order"] == "")
              input.merge!({"order" => get_new_order_id(customer)})
            end
            field = GoodData::Connectors::Metadata::Field.new({'hash' => input})
          elsif input.instance_of?(Field)
            field = input
          else
            raise MetadataExpcetion, "The fields which you are adding to fields collection, has incorrect type"
          end
          if (!customer.nil?)
            @customer_fields[customer] = {} if !@customer_fields.include?(customer)
            @customer_fields[customer][field.id] = field
          else
            @fields[field.id] = field
          end


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

        def get_enabled_fields(customer = nil)
          output = @fields.values.select { |v| !v.disabled? }.map { |v| v.id }
          if (!customer.nil? and @customer_fields.include?(customer))
            output += @customer_fields[customer].values.select {|v| !v.disabled?}.map {|v| v.id}
          end
          output
        end

        def get_enabled_fields_objects(customer = nil)
          output = @fields.values.select { |v| !v.disabled? }
          if (!customer.nil? and @customer_fields.include?(customer))
            output += @customer_fields[customer].values.select {|v| !v.disabled?}
          end
          output
        end

        def get_enabled_customer_fields_objects(customer)
          @customer_fields[customer].values.select {|v| !v.disabled?}
        end

        def get_customer_fields_objects(customer)
          @customer_fields[customer].values
        end

        def customer_have_fields?(customer)
          @customer_fields.include?(customer)
        end

        # def merge_customer_entity(customer_entity)
        #   customer_entity.custom.merge!(entity.custom) { |key, v1, v2| v1 }
        #   fields_to_add = entity.fields - customer_entity.fields
        #   fields_to_add.each do |field|
        #     # Calling to hash method, to make HARD COPY of the fields object (to be sure)
        #     customer_entity.add_field(field.to_hash)
        #   end
        #   customer_entity
        # end


        def merge!(entity)
          @custom.merge!(entity.custom){|k1,v1,v2| v1} unless entity.custom.nil?
          @name = entity.name if @name.nil?
          @dependent_on = entity.dependent_on if @dependent_on.nil? or @dependent_on.empty?

          fields_to_disable = @fields.values - entity.fields.values
          fields_to_add = entity.fields.values - @fields.values
          fields_to_merge = @fields.values & entity.fields.values

          if (!entity.fields.values.empty?)
            fields_to_disable.each do |field|
              field.disable('from entity settings')
            end

            fields_to_add.each do |field|
              field.order = get_new_order_id
              add_field(field)
            end

            fields_to_merge.each do |field|
              field.merge!(entity.get_field(field.id))
            end
          end
        end



        def diff_fields(fields_collection, customer = nil)
          changes = {}
          changes['only_in_source'] = []
          changes['only_in_target'] = []
          changes['changed'] = []

          fields_only_in_source,fields_only_in_target,fields_in_both_collections = []

          fields_to_compare = []
          if (!customer.nil?)
            if(@customer_fields.include?(customer))
              fields_to_compare = @customer_fields[customer].map{|k,v| v}
            end
          else
            fields_to_compare = @fields.values
          end

          fields_only_in_source = fields_to_compare - fields_collection
          fields_only_in_target = fields_collection - fields_to_compare
          fields_in_both_collections = fields_to_compare & fields_collection

          fields_only_in_source.each do |field|
            changes['only_in_source'] << field
          end

          fields_only_in_target.each do |field|
            changes['only_in_target'] << field
          end

          fields_in_both_collections.each do |field|
            hash = {}
            source_field = fields_to_compare.find{|f| f.id == field.id}
            target_field = fields_collection.find{|f| f.id == field.id}

            hash['name'] = { 'source' => source_field.name, 'target' => target_field.name } if source_field.name.downcase != target_field.name.downcase
            hash['type'] = { 'source' => source_field.type, 'target' => target_field.type } if source_field.type != target_field.type
            hash['disabled'] = { 'source' => source_field.disabled?, 'target' => target_field.disabled? } if (source_field.disabled? != target_field.disabled?)
            hash['source_field'] = source_field unless hash.empty?
            hash['target_field'] = target_field unless hash.empty?
            changes['changed'] << hash unless hash.empty?
          end
          changes
        end




        # This method with compare two entities.(It is done from perpective of first entity - entity on which the diff command is called)
        # It is currently comparing
        # Name
        # Disabled
        # Fields
        # IT will return the changes in hash
        def diff(entity)
          changes = {}
          changes['name'] = { 'source' => @name, 'target' => entity.name } if @name.downcase != entity.name.downcase
          changes['disabled'] = { 'source' => disabled?, 'target' => entity.disabled? } if disabled? != entity.disabled?
          changes['type'] = { 'source' => @type, 'target' => entity.type } if (@type.split('|') & entity.type.split('|')).count != @type.split('|').count
          changes['dependent_on'] = { 'source' => @dependent_on, 'target' => entity.dependent_on } if @dependent_on != entity.dependent_on

          changes['fields'] = diff_fields(entity.fields.values)
          changes
        end

        def get_new_order_id(customer = nil)
          prefix = ""
          max_id = 0
          if (!customer.nil?)
            prefix = "c"
            if (@customer_fields.include?(customer))
              @customer_fields[customer].each_pair do |key,field|
                value = Integer(field.order.gsub(prefix,""))
                max_id = value if value > max_id
              end
            end
          else
            prefix = "g"
            @fields.each_pair do |key,field|
              value = Integer(field.order.gsub(prefix,""))
              max_id = value if value > max_id
            end
          end
          "#{prefix}#{max_id+1}"
        end
      end
    end
  end
end
