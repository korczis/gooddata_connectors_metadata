# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'gooddata_connectors_metadata/version'

Gem::Specification.new do |spec|
  spec.name          = 'gooddata_connectors_metadata'
  spec.version       = GoodData::Connectors::Metadata::VERSION
  spec.authors       = ['Adrian Toman']
  spec.email         = ['adrian.toman@gooddata.com']
  spec.description   = %q{This is gem containing library for accessing gooddata connectors medatada}
  spec.summary       = ''
  spec.homepage      = ''
  spec.license       = 'MIT'

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ['lib']

  # Development dependencies
  spec.add_development_dependency 'bundler', '~> 1.3'
  spec.add_development_dependency 'coveralls', '~> 0.7', '>= 0.7.0'
  spec.add_development_dependency 'rake'
  spec.add_development_dependency 'rake-notes', '~> 0.2', '>= 0.2.0'
  spec.add_development_dependency 'rspec', '~> 3.1', '>= 3.1.0'
  spec.add_development_dependency 'rubocop', '~> 0.20.1'
  spec.add_development_dependency 'simplecov', '~> 0.8', '>= 0.8.2'

  # Regular dependencies
  spec.add_dependency 'gooddata', '~> 0.6.3'
  spec.add_dependency 'mongo', '~> 1.10.2'
  spec.add_dependency 'aws-sdk'
end
