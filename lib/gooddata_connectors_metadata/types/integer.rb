# encoding: UTF-8

module GoodData
  module Connectors
    module Metadata
      class IntegerType < BaseType


        def to_hash
          hash = {
              'type' => @type
          }
          hash.merge!({"autoincrement" => @autoincrement}) if !@autoincrement.nil?
        end

        def to_simple_string
          if (!@autoincrement.nil?)
            "#{@type}-#{@autoincrement}"
          else
            "#{@type}"
          end
        end

        def from_simple_string(string)
          values = string.split('-')
          @type = values[0]
          @autoincrement = to_bool(values[1]) if values.count == 2
        end

        def to_bool(value)
          return true if value =~ (/^(true|t|yes|y|1)$/i)
          return false if value == '' || value =~ (/^(false|f|no|n|0)$/i)
          fail TypeException, "Invalid value for boolean conversion: #{value}"
        end

        def from_hash(hash)
          if hash.include?('type')
            @type = hash['type']
          else
            fail TypeException, 'Some of the mandatory parameter for integer type are missing'
          end
          if hash.include?('autoincrement')
            @autoincrement = hash['autoincrement']
          end
        end

        def autoincrement? # rubocop:disable TrivialAccessors
          @autoincrement
        end

      end
    end
  end
end
