# encoding: UTF-8

module GoodData
  module Connectors
    module Metadata
      class Configuration
        class << self
          def load_from_files(folder)
            hash = {}
            files_in_directory = Dir["#{folder}/*.json"]
            files_in_directory.each do |file|
              begin
                config_leaf_name = file.split('/').last.gsub('.json', '')

                json = JSON.parse(File.read(file))
                hash[config_leaf_name] = json
                $log.info "Config for #{config_leaf_name} successfully loaded"
              rescue JSON::ParserError => _e
                raise MetadataException, "The parsing of file #{file} has failed. Most likely malformed JSON. Please inspect the configuration file"
              rescue => e
                raise MetadataException, "Unknown error where parsing JSON file. Message: #{e.message}"
              end
            end
            hash
          end

          # TO-DO this need to be changed, because it don't make sense
          def load_from_schedule(params)
            hash = {}
            # If the configuraion parameter have format (leaf_name)|key_name, we will put it to specific part of configuration
            params.each_pair do |key,value|
              if (key.split("|").count == 2)
                parsed_keys = key.split("|")
                hash[parsed_keys[0]] = {} if !hash.include?(parsed_keys[0])
                hash[parsed_keys[0]].merge!({parsed_keys[1] => value})
                puts "Adding #{parsed_keys[0]} #{parsed_keys[1]} #{value}"
              else
                hash[key] = value
              end
            end
            hash
          end

          def load_from_password_manager
          end
        end
      end
    end
  end
end
