module GoodData
  module Connectors
    module Metadata

        class Runtime

          attr_accessor :global,:entity


          class << self

            def fill(hash,options = {})
              if (hash.include?("global"))
                @global = hash["global"]
              else
                @global = {}
              end

              if (hash.include?("entity"))
                @entity = hash["entity"]
              else
                @entity = {}
              end
              @now = DateTime.now if @now.nil?
            end


            def get_entity(id)
              @entity[id]
            end

            def get_global(key)
              @global[key]
            end

            def set_entity_key_value(id,key,value)
              if (!@entity.include?(id))
                @entity[id] = {}
              end
              @entity[id][key] = value
            end

            def get_entity_value_by_key(id,key)
              if (@entity.include?(id))
                if (@entity[id].include?(key))
                  @entity[id][key] = value
                end
              end
            end


            def to_hash
              {
                "global" => @global,
                "entity" => @entity
              }
            end


            def get_load_id
              if (!@global.nil?)
                @global["load_id"] || 0
              else
                0
              end
            end

            def set_load_id(load_id)
              @global["load_id"] = load_id
            end

            def get_entity_last_load(id)
              if (@entity.include?(id))
                if (@entity[id].include?("last_load_date"))
                  DateTime.parse(@entity[id]["last_load_date"]) || nil
                end
              end
            end

            def set_entity_last_load(id,last_load_data)
              if (!@entity.include?(id))
                @entity[id] = {}
              end
              @entity[id]["last_load_date"]  = last_load_data.to_s
            end

            def now
              @now
            end




          end






        end

    end
  end
end