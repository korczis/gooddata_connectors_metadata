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
  spec.require_paths = ["lib"]

  spec.add_development_dependency 'bundler', '~> 1.3'
  spec.add_dependency 'gooddata', '~> 0.6.3'
  spec.add_dependency 'mongo', '~> 1.10.2'
  spec.add_development_dependency 'rake'
  spec.add_development_dependency 'rake-notes', '~> 0.2', '>= 0.2.0'
  spec.add_development_dependency 'rubocop', '~> 0.20.1'
end
