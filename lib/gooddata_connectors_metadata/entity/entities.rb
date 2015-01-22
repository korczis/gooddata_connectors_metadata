# encoding: UTF-8

module GoodData
  module Connectors
    module Metadata
      class Entities
        include Enumerable

        attr_accessor :entities
        attr_reader :entities
        # The configuration of entities will look like this
        # "entity_name":{
        #   "global":{
        #       global_entity settings
        #   },
        #   "customer1":{
        #       "customer specific "

        # }


        # }

        def initialize(options = {})
          @entities = {}
        end



        # def prepare_flat_collection
        #   collection = @entities.flat_map do |key,value|
        #     # No powered by
        #     if (value.keys.count == 1 and value.keys[0] == "global")
        #       value.values[0]
        #     else
        #       output = []
        #       value.values.each do |element|
        #         pp value
        #         if (element.customer != "global")
        #           output << element
        #         end
        #       end
        #       output
        #     end
        #   end
        #   collection
        # end




        def [](id)
          @entities[id]
        end

        def each(&block)
          @entities.values.each(&block)
        end

        # def each_flatted(&block)
        #   prepare_flat_collection.each(&block)
        # end

        # def each_customer(entity_name,&block)
        #   @entities[entity_name].find_all{|key,value| key != "global"}.values.each(&block)
        # end

        def include?(id)
          @entities.include?(id)
        end


        def <<(input_entity)
          @entities[input_entity.id] = {} if !@entities.include?(input_entity.id)
          @entities[input_entity.id]= input_entity
        end

        def get_entity_names
          @entities.keys
        end

        def to_hash
          output = []
          @entities.values.each do |entity|
            output << entity.to_hash
          end
          output
        end

        def get_entity_list_with_dependencies
          dependency_tree = {}
          root_elements = @entities.values.select { |v| v.dependent_on.nil? }
          root_elements.each do |v|
            dependency_tree[v.id] = []
          end
          depenedent_elements = @entities.values.select { |v| v.dependent_on }
          depenedent_elements.each do |v|
            if dependency_tree.include?(v.dependent_on)
              dependency_tree[v.dependent_on] << v.id
            else
              fail EntityException, "Error in generating the entity dependency tree. Entity #{v.id} with dependency setting #{v.dependent_on} don't have root element"
            end
          end
          dependency_tree
        end
      end
    end
  end
end
