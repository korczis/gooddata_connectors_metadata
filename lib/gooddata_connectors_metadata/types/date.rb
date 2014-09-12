module GoodData
  module Connectors
    module Metadata

      class DateType < BaseType

        def to_hash
          {
              "type" => @type,
              "with_time" => @with_time
          }
        end

        def to_simple_string
           "#{@type}-#{@with_time}"
        end

        def from_simple_string(string)
          values = string.split("-")
          @type = values[0]
          @with_time = to_bool(values[1])
        end

        def to_bool(value)
          return true if value =~ (/^(true|t|yes|y|1)$/i)
          return false if value == "" || value =~ (/^(false|f|no|n|0)$/i)
          raise TypeException.new "Invalid value for boolean conversion: #{value}"
        end

        def from_hash(hash)
          if (hash.include?("type"))
            @type = hash["type"]
          else
            raise TypeException, "Some of the mandatory parameter for date type are missing"
          end
          @with_time = hash["with_time"] || false
        end

        def with_time?
          @with_time
        end
       end
    end
  end
end