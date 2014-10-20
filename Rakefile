require 'bundler/gem_tasks'

require 'rake/testtask'
require 'rake/notes/rake_task'
require 'rspec/core/rake_task'

RSpec::Core::RakeTask.new(:test)

desc 'Run Rubocop'
task :cop do
  exec 'rubocop lib/'
end

desc 'Run continuous integration test'
task :ci do
  Rake::Task['test:unit'].invoke
  unless ENV['TRAVIS'] == 'true' && ENV['TRAVIS_SECURE_ENV_VARS'] == 'false'
    Rake::Task['test:integration'].invoke
  end

  # Rake::Task['test:cop'].invoke if RUBY_VERSION.start_with?('2.2') == false
  # Rake::Task['coveralls:push'].invoke
end

namespace :test do
  desc 'Run unit tests'
  RSpec::Core::RakeTask.new(:unit) do |t|
    t.pattern = 'spec/unit/**/*.rb'
  end

  desc 'Run integration tests'
  RSpec::Core::RakeTask.new(:integration) do |t|
    t.pattern = 'spec/integration/**/*.rb'
  end

  desc 'Run legacy tests'
  RSpec::Core::RakeTask.new(:legacy) do |t|
    t.pattern = 'test/**/test_*.rb'
  end

  desc 'Run coding style tests'
  RSpec::Core::RakeTask.new(:cop) do |t|
    Rake::Task['cop'].invoke
  end

  task :all => [:unit, :integration] # :cop
end

desc 'Run all tests'
task :test => 'test:all'

task :usage do
  puts 'No rake task specified, use rake -T to list them'
end

task :default => [:usage]