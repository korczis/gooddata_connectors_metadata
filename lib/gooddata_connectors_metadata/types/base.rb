# encoding: UTF-8

module GoodData
  module Connectors
    module Metadata
      class BaseType
        attr_accessor :type, :size

        def initialize(input)
          if input.instance_of? String
            from_simple_string(input)
          elsif input.instance_of?(Hash) || input.instance_of?(BSON::OrderedHash)
            from_hash(input)
          end
        end

        def ==(other)
          to_simple_string == other.to_simple_string
        end

        def to_simple_string
          if @size > 0
            "#{@type}-#{@size}"
          else
            "#{@type}"
          end
        end

        def from_simple_string(string)
          values = string.split('-')
          @type = values[0]
          if values.count > 1
            @size = Integer(values[1])
          else
            @size = 0
          end
        end

        def to_hash
          if @size > 0
            {
              'type' => @type,
              'size' => @size
            }
          else
            {
              'type' => @type
            }
          end
        end

        def from_hash(hash)
          if hash.include?('type')
            @type = hash['type']
          else
            fail TypeException, 'The hash need to have type value'
          end
          @size = hash['size'] if hash.include?('size')
        end

        def default
          nil
        end

        def nullabble?
          true
        end


        @subtypes = nil
        @subtypes_factory = nil

        class << self
          def create(input)
            type = nil
            if input.instance_of? String
              type = input.split('-')[0]
            elsif input.instance_of?(Hash)
              type = input['type']
            end

            # Init factory if required
            create_subtypes_factory unless @subtypes_factory
            fail ArgumentError, "Bad log file type: #{input}" unless @subtypes_factory.key?(type)
            klass = @subtypes_factory[type]
            klass.new(input)
          end

          def create_subtypes_factory
            res = {}
            get_subtypes unless @subtypes
            @subtypes.each do |subtype|
              name = subtype.to_s.split('::').last.downcase.gsub(/type$/, '')
              res[name] = subtype
            end
            @subtypes_factory = res
          end

          def get_subtypes
            @subtypes = list_subtypes unless @subtypes
            @subtypes
          end

          def get_subtypes_factory
            create_subtypes_factory unless @subtypes_factory
            @subtypes_factory
          end

          def get_subtypes_names
            get_subtypes_factory.keys
          end

          def list_subtypes
            ObjectSpace.each_object(Class).select { |klass| klass < self }
          end
        end


      end
    end
  end
end
