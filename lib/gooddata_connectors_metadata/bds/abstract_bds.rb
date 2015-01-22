require 'json'

# Abstract BDS class
module GoodData
  module Connectors
    module Metadata
      class AbstractBds


        attr_accessor :context

        def initialize(options = {})
          @option = options
          # Copy context settings to own hash
          @context = MetadataContext.new(options[:default_folder],options[:account_id],options[:token],options[:data_source])
        end

        def store(path, content,metedata)
          fail MetadataException, "Method must be implemented by descendant"
        end

        def get(remote_path,local_path)
          fail MetadataException, "Method must be implemented by descendant"
        end

        def ls(path)
          fail MetadataException, "Method must be implemented by descendant"
        end

        def exists?(path)
          fail MetadataException, "Method must be implemented by descendant"
        end


        def check_context(options = @context)
          check = !options[:account_id].nil? && !options[:account_id].empty?
          fail MetadataException, 'No account_id specified' unless check

          check = !options[:token].nil? && !options[:token].empty?
          fail MetadataException, 'No token specified' unless check

          check = !options[:entity].nil? && !options[:entity].empty?
          fail MetadataException, 'No entity specified' unless check
        end


        def store_data(entity_name,file,remote_filename,time,metadata = nil)
          path = @context.construct_full_data_path(entity_name,remote_filename,time)
          store(path,file,metadata)
        end


        def store_metadata(entity,file)
          path = @context.construct_full_data_path(entity,"metadata")
          store(path,file)
        end

        def load_entity_metadata(entity,timestamp,year,month,day,customer = nil)
          path = @context.construct_full_metadata_path(entity,"#{!customer.nil? ? customer + "/" : ""}#{timestamp}_metadata.json",Time.new(year,month,day))
          local_file_name = "metadata/#{!customer.nil? ? "/" + customer : ""}#{@context.entity}_#{timestamp}_metadata.json"
          get(path,local_file_name)
          if (File.exists?(local_file_name))
            JSON.parse(File.read(local_file_name))
          else
            {}
          end
        end


        def load_configuration
          FileUtils.mkdir_p("metadata")
          get(@context.construct_path(:two,"configuration.json"),Metadata::CONFIGURATION_PATH)
          if (File.exists?(Metadata::CONFIGURATION_PATH))
            hash = JSON.parse(File.read(Metadata::CONFIGURATION_PATH))
          else
            hash = {}
          end
          hash
        end

        def load_source_metadata
          FileUtils.mkdir_p("metadata")
          get(@context.construct_path(:three,"source.json"),Metadata::SOURCE_METADATA_PATH)
          if (File.exists?(Metadata::SOURCE_METADATA_PATH))
            hash = JSON.parse(File.read(Metadata::SOURCE_METADATA_PATH))
          else
            hash = {}
          end
          hash
        end



        def find_last_element_in_path(path,entity_name,customer = nil,max_business_date = Time.now.utc)
          n = 3
          begin
            elements = ls(path)
            elements.delete_if{|element| element.last == "/"}
            if !elements.empty?
              elements = elements.map do |e|
                values = e.split('/')
                # We need to find where the entity name is in the BDS Path. This is needed because of customer specific data
                entity_name_index = values.index(entity_name)
                if (customer.nil?)
                  if (values.count - entity_name_index == 5)
                    {
                        :timestamp =>  values[entity_name_index+4].split('_')[0],
                        :year => values[entity_name_index+1],
                        :month => values[entity_name_index+2],
                        :day => values[entity_name_index+3]
                    }
                  else
                    nil
                  end
                else
                  if (values.count - entity_name_index == 6 and values[entity_name_index+4] == customer )
                    {
                        :timestamp =>  values[entity_name_index+5].split('_')[0],
                        :year => values[entity_name_index+1],
                        :month => values[entity_name_index+2],
                        :day => values[entity_name_index+3]
                    }
                  else
                    nil
                  end
                end
              end
              elements.compact!
              elements = elements.sort_by{|e| e[:timestamp]}
              lowest = elements.partition{ |e| Time.strptime(e[:timestamp],"%s") <= max_business_date }[0][-1]
              if lowest.nil?
                # the elements are found in the same period, however all are larger than the max_business_date_epoch
                nil
              else
                return lowest
              end
            end
            path = remove_last_element(path, '/')
            n-=1
          end while elements.empty? and n >= 0
        end


        def last_business_date(entity_name,customer = nil,max_business_date = Time.now.utc)
          path = @context.construct_full_data_path(entity_name,"",max_business_date)
          find_last_element_in_path(path,entity_name,customer,max_business_date)
        end

        def last_metadata_date(entity_name,customer = nil,max_business_date = Time.now.utc)
          path = @context.construct_full_metadata_path(entity_name,"",max_business_date)
          find_last_element_in_path(path,entity_name,customer,max_business_date)
        end

        def remove_last_element(path,char)
            list = path.split(char)
            list.delete_at(list.count - 1)
            list.join(char)
        end

        def timestamp_to_time(timestamp)
          if (!timestamp.nil?)
            Time.strptime(timestamp,"%s")
          else
            nil
          end
        end


        def get_entity_metadata(entity,customer = nil,options = {})
          element = last_metadata_date(entity,customer)
          if (!element.nil? and !element.empty?)
            $log.info "The last found metadata element is with settings year:#{element[:year]}, month:#{element[:month]}, day:#{element[:day]}, timestamp:#{element[:timestamp]}"
            begin
              metadata = load_entity_metadata(entity,element[:timestamp],element[:year],element[:month],element[:day],customer)
              {
                  :metadata => metadata,
                  :time_element => element
              }
            rescue AWS::S3::Errors::NoSuchKey => e
              raise MetadataException,"There are no metadata corresponding the data file. Looks like previous run has finished in incosistent state. Please contact support. #{element.to_s}"
            rescue => e
              raise MetadataException, e.message
            end
          else
            {
                :metadata => nil
            }
          end

        end

      end
    end
  end
end