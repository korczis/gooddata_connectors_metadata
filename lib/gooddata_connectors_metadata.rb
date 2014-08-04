require "gooddata_connectors_metadata/version"
require "mongo"
require "json"
require "gooddata"

require_relative "gooddata_connectors_metadata/metadata"
%w(metadata_exception missing_parameters).each {|file| require_relative "gooddata_connectors_metadata/exceptions/#{file}"}
require_relative "gooddata_connectors_metadata/configuration/configuration"

module GoodDataConnectorsMetadata

    class MetadataMiddleware < GoodData::Bricks::Middleware
      def call(params)
        $log = params["GDC_LOGGER"]
        metadata_options = params["metadata"]

        $log.info "Initilizing metadata storage"
        metadata = Metadata.new(metadata_options)

        #This section will handle default metadata load
        schedule_id = params["SCHEDULE_ID"]
        if !schedule_id.nil?
          $log.info "Loading global metadata for schedule #{schedule_id}"
          metadata.load_global_hash(schedule_id)
        end
        @app.call(params.merge('metadata_wrapper' => metadata))
      end
    end
end
