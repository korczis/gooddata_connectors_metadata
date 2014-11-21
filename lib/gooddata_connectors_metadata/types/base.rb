# encoding: UTF-8

module GoodData
  module Connectors
    module Metadata
      class BaseType
        attr_accessor :type, :size

        def initialize(input)
          if input.instance_of? String
            from_simple_string(input)
          elsif input.instance_of?(Hash) || input.instance_of?(BSON::OrderedHash)
            from_hash(input)
          end
        end

        def ==(other)
          to_simple_string == other.to_simple_string
        end

        def to_simple_string
          if @size > 0
            "#{@type}-#{@size}"
          else
            "#{@type}"
          end
        end

        def from_simple_string(string)
          values = string.split('-')
          @type = values[0]
          if values.count > 1
            @size = Integer(values[1])
          else
            @size = 0
          end
        end

        def to_hash
          if @size > 0
            {
              'type' => @type,
              'size' => @size
            }
          else
            {
              'type' => @type
            }
          end
        end

        def from_hash(hash)
          if hash.include?('type')
            @type = hash['type']
          else
            fail TypeException, 'The hash need to have type value'
          end
          @size = hash['size'] if hash.include?('size')
        end

        def self.create(input)
          type = nil
          if input.instance_of? String
            type = input.split('-')[0]
          elsif input.instance_of?(Hash) || input.instance_of?(BSON::OrderedHash)
            type = input['type']
          end
          case type
          when 'date'
            DateType.new(input)
          when 'boolean'
            BooleanType.new(input)
          when 'decimal'
            DecimalType.new(input)
          when 'integer'
            IntegerType.new(input)
          when 'string'
            StringType.new(input)
          else
            fail ArgumentError, "Bad type: #{input}"
          end
        end

        def default
          nil
        end

        def nullabble?
          true
        end
      end
    end
  end
end
