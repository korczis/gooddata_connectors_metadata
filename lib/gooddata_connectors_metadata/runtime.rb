# encoding: UTF-8

module GoodData
  module Connectors
    module Metadata
      class Runtime
        attr_accessor :global, :entity
        class << self
          def fill(options = {})
            @now = Time.now if @now.nil?
          end

          def get_entity(id)
            @entity[id]
          end

          def get_global(key)
            @global[key]
          end

          def get_load_id
            if @global
              @global['load_id'] || 0
            else
              0
            end
          end

          def set_load_id(load_id)
            @global['load_id'] = load_id
          end

          def get_entity_last_load(id)
            if @entity.include?(id)
              if @entity[id].include?('last_load_date')
                DateTime.parse(@entity[id]['last_load_date']) || nil
              end
            end
          end

          def set_entity_last_load(id, last_load_data)
            @entity[id] = {} unless @entity.include?(id)
            @entity[id]['last_load_date'] = last_load_data.to_s
          end

          def now # rubocop:disable TrivialAccessors
            @now.utc
          end

          def now_timestamp
            @now.utc.strftime("%s")
          end
        end
      end
    end
  end
end
