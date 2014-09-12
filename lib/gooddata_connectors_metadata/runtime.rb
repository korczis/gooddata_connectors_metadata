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
            end


            def get_entity(id)
              @entity[id]
            end

            def get_global(key)
              @global[key]
            end

            def to_hash
              {
                "global" => @global,
                "entity" => @entity
              }
            end


            def get_load_id
              @global["load_id"] || 0
            end

            def set_load_id(load_id)
              @global["load_id"] = load_id
            end


          end






        end

    end
  end
end