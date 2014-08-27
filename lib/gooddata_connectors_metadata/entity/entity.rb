module GoodDataConnectorsMetadata

  class Entity
    attr_accessor :id,:name,:custom, :fields

    #{
    #    "id" => "id",
    #    "name" => "name",
    #    "type" => "type",
    #    "custom" => {
    #        "test1" => "value"
    #        etc
    #    }
    #}


    # Requesting id, name, type (optional custom)
    # or
    # hash
    def initialize(args = {})
      @fields = {}
      if (!args["hash"].nil?)
        from_hash(args["hash"])
      elsif(!args["id"].nil?)
        @id = args["id"]
        @name = args["name"] || args["id"]
        @history = args["history"] || {}
        @custom = args["custom"] unless (args["custom"].nil?)
        @enabled = args["enabled"] || true
        unless (args["fields"].nil?)
          args["fields"].each do |field|
            add_field(field)
          end
        end
      else
        pp args
        raise EntityException, "Missing mandatory parameters when creating entity, mandatory fields are id,name,type or hash"
      end
    end


    def ==(entity)
      @id == entity.id
    end

    def to_hash
      {
          "id" => @id,
          "name" => @name,
          "custom" => @custom,
          "fields" => @fields.map {|k,v| v.to_hash},
          "history" => @history,
          "enabled" => @enabled
      }
    end

    def from_hash(hash)
      @id = hash["id"]
      @name = hash["name"]
      @custom = hash["custom"] unless hash["custom"].nil?
      @history = hash["history"] || {}
      @enabled = hash["enabled"] || true
      unless (hash["fields"].nil?)
        hash["fields"].each do |field_element|
          # Lets process the fields from the configuration file
          # The fields could be in form of Array or in form of HASH with type
          # Array  - ["Name","Id","Stage"]
          # Hash - [
          #  {"id" => "Name","type" => "string-18"}
          #  {"id" => "Id","type" => "string-18"}
          #  {"id" => "Stage","type" => "string-10"}
          #  ]
          if field_element.instance_of?(String)
            field = Field.new({"id" => field_element})
          elsif ((field_element.instance_of?(Hash) or field_element.instance_of?(BSON::OrderedHash)) and (field_element.include?("id")))
            field = Field.new({"hash" => field_element})
          else
            raise MetadataException, "Wrong parsing of field"
          end
          @fields[field.id] = field
        end
      end
    end

    def field_exist?(name)
      @fields.include?(name)
    end

    def get_field(name)
      @fields[name]
    end

    def delete_field(name,reason = "")
      @fields[name].disable
      @fields[name].custom["disable_reason"] = reason
    end


    def get_ids
      fields.map {|f| f.id}
    end

    def add_field(input)
      if (input.instance_of?(String))
        field = Field.new({"id" => input})
      elsif (input.instance_of?(Hash))
        field = Field.new({"hash" => input})
      elsif (input.instance_of?(Field))
        field = input
      end
      @fields[field.id] = field
    end

    def add_history_field(key,value)
      if (!@history.include?($LOAD_ID))
        @history[$LOAD_ID] = {}
      end
      @history[$LOAD_ID].merge!({key => value})
    end

    def get_history_field(key,load_id = $LOAD_ID)
      if (@history.include?(load_id))
        @history[load_id][key]
      end
    end

    def disable(reason = "")
      @enabled = false
      @custom["disable_reason"] = reason
    end

    def disabled?
      !@enabled
    end

    def get_enabled_fields
      @fields.values.find_all{|v| !v.disabled?}.map{|v| v.id}
    end


    def merge!(entity,enable_add = true)
      @custom.merge! entity.custom unless entity.custom.nil?
      @name = entity.name unless entity.name.nil?
      fields_to_disable = @fields.values - entity.fields.values
      fields_to_add = entity.fields.values - @fields.values
      fields_to_merge = @fields.values & entity.fields.values

      fields_to_disable.each do |field|
        field.disable("from merge")
      end

      if (enable_add)
        fields_to_add.each do |field|
          add_field(field)
        end
      end

      fields_to_merge.each do |field|
        field.merge! entity.get_field(field.id)
      end
    end

  end

end