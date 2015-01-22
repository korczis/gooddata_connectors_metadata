# encoding: UTF-8

module GoodData
  module Connectors
    module Metadata
      class Field
        include Comparable

        attr_accessor :id, :name, :type, :custom, :enabled,:order

        # Requesting id,name, type and custom
        # or
        # hash
        def initialize(args)
          @enabled = true
          if args['hash']
            from_hash(args['hash'])
          elsif args['id']
            @id = args['id']
            @name = args['name'] || args['id']
            if args['type']
              @type = BaseType.create(args['type'])
            else
              @type = BaseType.create('string-255')
            end
            @custom = args['custom'] || {}
            @enabled = args['enabled'].nil? ? true : args['enabled']
            @order = args["order"] || ""
          else
            fail EntityException, 'Missing mandatory parameters when creating fields, mandatory fields are id,name,type or hash'
          end
        end

        def <=>(other)
          @id.downcase <=> other.id.downcase
        end

        def hash
          @id.downcase.hash
        end

        alias eql? ==


        def to_hash
          {
            'id' => @id,
            'name' => @name,
            'order' => @order,
            'type' => @type.to_simple_string,
            'custom' => @custom,
            'enabled' => @enabled
          }
        end

        def from_hash(hash)
          @id = hash['id']
          @name = hash['name'] || hash["id"]
          @order = hash['order']
          @type = BaseType.create(hash['type'])
          @custom = hash['custom'] || {}
          @enabled = hash['enabled'].nil? ? true : hash['enabled']
        end

        def disable(reason = '')
          @enabled = false
          @custom['disable_reason'] = reason
        end

        def disabled?
          !@enabled
        end

        def custom_field?
          @order[0] == "c"
        end


        def merge!(field)
          @name = field.name if @name.nil?
          @type = field.type if @type.nil?
          @custom.merge!(field.custom){|k1,v1,v2| v1}
        end
      end
    end
  end
end
