module GoodDataConnectorsMetadata

  class BaseType

    attr_accessor :type,:size


    def initialize(input)
      if (input.instance_of? String)
        from_simple_string(input)
      elsif (input.instance_of? Hash)
        from_hash(input)
      end
    end


    def ==(obj)
      to_simple_string == obj.to_simple_string
    end

    def to_simple_string
      if (@size > 0)
        "#{@type}-#{@size}"
      else
        "#{@type}"
      end
    end

    def from_simple_string(string)
      values = string.split("-")
      @type = values[0]
      if (values.count > 1)
        @size = Integer(values[1])
      else
        @size = 0
      end
    end


    def to_hash
      if (@size > 0)
        {
            "type" => @type,
            "size" => @size
        }
      else
        {
            "type" => @type
        }
      end
    end

    def from_hash(hash)
      if (hash.include?("type"))
        @type = hash["type"]
      else
        raise TypeException,"The hash need to have type value"
      end
      if (hash.include?("size"))
        @size = hash["size"]
      end
    end


    def self.create input
      type = nil
      if (input.instance_of? String)
        type = input.split("-")[0]
      elsif (input.instance_of? Hash)
        type = input["type"]
      end
      case type
        when "date"
          DateType.new(input)
        when "boolean"
          BooleanType.new(input)
        when "decimal"
          DecimalType.new(input)
        when "integer"
          IntegerType.new(input)
        when "string"
          StringType.new(input)
        else
          raise "Bad log file type: #{input.to_s}"
      end
    end










  end


end