# encoding: UTF-8
module GoodData
  module Connectors
    module Metadata
      # Time Helper
      module TimeHelper

        class << self

          def day(time)
            time.day.to_s.rjust(2,'0')
          end

          def month(time)
            time.month.to_s.rjust(2,'0')
          end

          def year(time)
            time.year.to_s
          end


        end
      end
    end
  end
end

