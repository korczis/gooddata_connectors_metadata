# encoding: UTF-8

module GoodData
  module Connectors
    module Metadata
      class StringType < BaseType
        def default
          nil
        end

        def nullabble?
          false
        end
      end
    end
  end
end