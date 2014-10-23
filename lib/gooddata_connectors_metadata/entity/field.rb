# encoding: UTF-8

module GoodData
  module Connectors
    module Metadata
      class Field
        include Comparable

        attr_accessor :id, :name, :type, :custom, :enabled, :history

        # Requesting id,name, type and custom
        # or
        # hash
        def initialize(args = {})
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
            @history = args['history'] if args['history']
            @enabled = args['enabled'] || true
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
            'type' => @type.to_simple_string,
            'custom' => @custom,
            'history' => @history,
            'enabled' => @enabled
          }
        end

        def from_hash(hash)
          @id = hash['id']
          @name = hash['name']
          @type = BaseType.create(hash['type'])
          @custom = hash['custom'] || {}
          @history = hash['history'] unless hash['history'].nil?
          @enabled = hash['enabled'] || true
        end

        def disable(reason = '')
          @enabled = false
          @custom['disable_reason'] = reason
        end

        def disabled?
          !@enabled
        end

        def merge!(field, reenable = false)
          @name = field.name unless field.name.nil?
          @type = field.type unless field.type.nil?
          @enabled = field.enabled if reenable
          @custom.merge! field.custom
        end
      end
    end
  end
end
