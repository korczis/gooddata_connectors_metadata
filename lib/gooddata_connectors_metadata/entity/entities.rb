# encoding: UTF-8

module GoodData
  module Connectors
    module Metadata
      class Entities
        include Enumerable

        def initialize(args = {})
          @entities = {}
          if !args.empty? && args.include?('hash')
            args['hash'].each do |entity_hash|
              entity = Entity.new('hash' => entity_hash)
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
          if input_entity.instance_of?(Entity)
            if !@entities.include?(input_entity.id)
              @entities[input_entity.id] = input_entity
            else
              @entities[input_entity.id].merge! (input_entity)
            end
          elsif input_entity.instance_of?(Hash)
            entity = Entity.new('hash' => input_entity)
            if !@entities.include?(entity.id)
              @entities[entity.id] = entity
            else
              @entities[input_entity.id].merge! (entity)
            end
          else
            fail EntityException, 'Unsuported type of input object. Supported types Hash,Entity'
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
            output
        end

        def get_entity_list_with_dependencies
          dependency_tree = {}
          root_elements = @entities.values.find_all{|v| v.dependent_on.nil?}
          root_elements.each do |v|
            dependency_tree[v.id] = []
          end
          depenedent_elements = @entities.values.find_all{|v| !v.dependent_on.nil?}
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
