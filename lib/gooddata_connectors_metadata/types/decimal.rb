# encoding: UTF-8

module GoodData
  module Connectors
    module Metadata
      class DecimalType < BaseType
        attr_accessor :size_before_comman, :size_after_comma

        def to_simple_string
          "#{@type}-#{@size}-#{@size_after_comma}"
        end

        def from_simple_string(string)
          values = string.split('-')
          @type = values[0]
          @size = Integer(values[1])
          @size_after_comma = Integer(values[2])
        end

        def to_hash
          {
            'type' => @type,
            'size_after_comma' => @size_after_comma,
            'size' => @size
          }
        end

        def from_hash(hash)
          if hash.include?('type') && hash.include?('size_after_comma') && hash.include?('size')
            @type = hash['type']
            @size_after_comma = hash['size_after_comma']
            @size = hash['size']
          else
            fail TypeException, 'Some of the mandatory parameter for decimal type are missing'
          end
        end
      end
    end
  end
end
