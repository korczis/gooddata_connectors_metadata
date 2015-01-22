module GoodData
  module Connectors
    module Metadata
      class MetadataContext

        attr_accessor :account_id,:token,:data_source,:entity,:customer

        def initialize(default_folder,account_id,token,data_source = nil)
          @default_folder = default_folder
          @account_id = account_id
          @token = token
          @data_source = data_source
        end


        def construct_full_data_path(entity,path, time = Time.new.utc)
          File.join(
              @default_folder,
              @account_id,
              @token,
              @data_source,
              entity,
              TimeHelper.year(time),
              TimeHelper.month(time),
              TimeHelper.day(time),
              path
          )
        end

        def construct_full_metadata_path(entity,path, time = Time.new.utc)
          File.join(
              @default_folder,
              @account_id,
              @token,
              "metadata",
              entity,
              TimeHelper.year(time),
              TimeHelper.month(time),
              TimeHelper.day(time),
              path
          )
        end



        def construct_path(level,path)
          case level
            when :one
              File.join(@default_folder,@account_id,path)
            when :two
              File.join(@default_folder,@account_id,@token,path)
            when :three
              File.join(@default_folder,@account_id,@token,@data_source,path)
          end
        end

      end
    end
  end
end