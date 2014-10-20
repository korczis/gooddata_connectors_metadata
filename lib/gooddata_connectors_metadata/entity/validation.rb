# encoding: UTF-8

module GoodData
  module Connectors
    module Metadata
      class Validation
        attr_accessor :type, :template, :value

        def initialize(type, template, custom = {})
          @type = type
          @template = template
          @custom = custom
        end
      end
    end
  end
end