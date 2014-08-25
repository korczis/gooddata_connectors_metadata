module GoodDataConnectorsMetadata

  class Field

    include Comparable

    attr_accessor :id,:name,:type,:custom, :enabled, :history


    # Requesting id,name, type and custom
    # or
    # hash
    def initialize(args = {})
      @enabled = true
      if (!args["hash"].nil?)
        from_hash(args["hash"])
      elsif(!args["id"].nil?)
        @id = args["id"]
        @name = args["name"] || args["id"]
        if(!args["type"].nil?)
          @type = BaseType.create(args["type"])
        else
          @type = BaseType.create("string-255")
        end
        @custom = args["custom"] || {}
        @history = args["history"] unless (args["history"].nil?)
        @enabled = args["enabled"] || true
      else
        raise EntityException, "Missing mandatory parameters when creating fields, mandatory fields are id,name,type or hash"
      end
    end

    def <=>(obj)
      @id <=> obj.id
    end

    def hash
      @id.hash
    end
    alias eql? ==

    def to_hash
      {
          "id" => @id,
          "name" => @name,
          "type" => @type.to_hash,
          "custom" => @custom,
          "history" => @history,
          "enabled" => @enabled
      }
    end

    def from_hash(hash)
       @id = hash["id"]
       @name = hash["name"]
       @type = BaseType.create(hash["type"])
       @custom = hash["custom"] || {}
       @history = hash["history"] unless hash["history"].nil?
       @enabled = hash["enabled"] || true
    end

    def disable(reason = "")
      @enabled = false
      @custom["disable_reason"] = reason
    end

    def disabled?
      !@enabled
    end

    def merge!(field)
      @name = field.name unless field.name.nil?
      @type = field.type unless field.type.nil?
      @custom.merge! field.custom
    end



  end




end