require 'aws-sdk-v1'

# AWS S3 BDS
module GoodData
  module Connectors
    module Metadata
      class S3Bds < AbstractBds

        # S3 connection
        @s3 = nil

        def initialize(options = {})
          super(options)
          @logger = options[:logger] || Logger.new(STDOUT)
          AWS.config({
                         :access_key_id => options[:key],
                         :secret_access_key => options[:secret],
                         :log_level => :info,
                         :logger => @logger,
                         :max_retries => 3
                     })
          @s3 = AWS::S3.new
          @bucket = @s3.buckets[options[:bucket]]

        end

        def store(full_remote_path, content,metadata = nil)
          @logger.info "Uploading file to path #{full_remote_path}."
          begin
            obj = @bucket.objects[full_remote_path]
            #obj.metadata["test"] = content[:metadata]
            if (metadata.nil?)
              obj.write(File.open(content[:file],"rb"))
            else
              obj.write(File.open(content[:file],"rb"),:metadata => metadata)
            end
          rescue Exception => e
            return {
                :status => :failed,
                :path => full_remote_path,
                :reason => e
            }
          end
          {
              :status => :ok,
              :path => full_remote_path
          }
        end


        def get (remote_path,local_path)
          obj = @bucket.objects[remote_path]
          folder = local_path.split('/')[0...-1].join('/')
          FileUtils.mkdir_p(folder) unless File.directory?(folder)
          metadata = nil
          File.open(local_path, 'wb') do |file|
            metadata = obj.metadata
            obj.read do |chunk|
              file.write(chunk)
            end
          end
          metadata
        end

        def ls(path)
          @bucket.objects.with_prefix(path).map(&:key)
        end

        def exists?(path)
          @bucket.objects[path].exists?
        end

      end
    end
  end
end