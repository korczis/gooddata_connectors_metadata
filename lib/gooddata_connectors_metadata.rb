# encoding: UTF-8

require 'gooddata_connectors_metadata/version'
require 'mongo'
require 'json'
require 'gooddata'

require_relative 'gooddata_connectors_metadata/metadata'
require_relative 'gooddata_connectors_metadata/metadata_context'
require_relative 'gooddata_connectors_metadata/runtime'

# TODO: List files in directory
%w(metadata_exception missing_parameters entity_exception type_exception).each { |file| require_relative "gooddata_connectors_metadata/exceptions/#{file}" }

# TODO: List files in directory
%w(metadata_exception missing_parameters).each { |file| require_relative "gooddata_connectors_metadata/exceptions/#{file}" }

# TODO: List files in directory
%w(entities entity field validation).each { |file| require_relative "gooddata_connectors_metadata/entity/#{file}" }

# TODO: List files in directory
%w(base boolean date decimal integer string).each { |file| require_relative "gooddata_connectors_metadata/types/#{file}" }
%w(time_helper abstract_bds s3_bds).each { |file| require_relative "gooddata_connectors_metadata/bds/#{file}" }
require_relative 'gooddata_connectors_metadata/configuration/configuration'

module GoodData
  module Connectors
    module Metadata
      class MetadataMiddleware < Bricks::Middleware
        def call(params)
          $log = params['GDC_LOGGER']

          $log.info 'Initilizing metadata storage'
          metadata = Metadata.new(params)

          @app.call(params.merge!("metadata_wrapper" => metadata))
        end
      end
    end
  end
end
