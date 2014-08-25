module GoodDataConnectorsMetadata

  class Entities
    include Enumerable

    def initialize(args = {})
      @entities = {}
      if (!args.empty? and !args.include?("hash"))
        args["hash"]["entities"].each do |entity_hash|
          entity = Entity.new({"hash" => entity_hash})
          @entities[entity.id] = entity
        end
      end
    end


    def [](id)
      @entities[id]
    end

    def each(&block)
      @entities.values.each(&block)
    end


    def include?(id)
      @entities.include?(id)
    end

    def <<(input_entity)
      if (input_entity.instance_of?(Entity))
        if (!@entities.include?(input_entity.id))
          @entities[input_entity.id] = input_entity
        else
          @entities[input_entity.id].merge! (input_entity)
        end
      elsif input_entity.instance_of?(Hash)
        entity = Entity.new({ "hash" => input_entity})
        if (!@entities.include?(entity.id))
          @entities[entity.id] = entity
        else
          @entities[input_entity.id].merge! (entity)
        end
      else
        raise EntityException, "Unsuported type of input object. Supported types Hash,Entity"
      end
    end


    def get_entity_names
      @entities.keys
    end

    def to_hash
      output = []
      @entities.values.each do |entity|
        output << entity.to_hash
      end
      {
          "entities" => output
      }
    end


  end




end