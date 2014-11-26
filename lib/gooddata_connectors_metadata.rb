# encoding: UTF-8

require 'gooddata_connectors_metadata/version'
require 'mongo'
require 'json'
require 'gooddata'

require_relative 'gooddata_connectors_metadata/metadata'
require_relative 'gooddata_connectors_metadata/runtime'

# TODO: List files in directory
%w(metadata_exception missing_parameters entity_exception type_exception).each { |file| require_relative "gooddata_connectors_metadata/exceptions/#{file}" }

# TODO: List files in directory
%w(metadata_exception missing_parameters).each { |file| require_relative "gooddata_connectors_metadata/exceptions/#{file}" }

# TODO: List files in directory
%w(entities entity field validation).each { |file| require_relative "gooddata_connectors_metadata/entity/#{file}" }

# TODO: List files in directory
%w(base boolean date decimal integer string).each { |file| require_relative "gooddata_connectors_metadata/types/#{file}" }
require_relative 'gooddata_connectors_metadata/configuration/configuration'

module GoodData
  module Connectors
    module Metadata
      class MetadataMiddleware < Bricks::Middleware
        def call(params)
          $log = params['GDC_LOGGER']

          $log.info 'Initilizing metadata storage'
          metadata = Metadata.new(params)
          # This section will handle default metadata load


          fail MetadataException, 'The variable SCHEDULE_ID is not present in metadata initialization call' if params['SCHEDULE_ID'].nil?

          # TODO: Eliminate $SCHEDULE_ID global variable
          if params['SCHEDULE_ID']
            $log.info "Loading global metadata for schedule #{params['SCHEDULE_ID']}"
            metadata.load_global_hash(params['SCHEDULE_ID'])
          end
          @app.call(params.merge('metadata_wrapper' => metadata))
        end
      end
    end
  end
end
