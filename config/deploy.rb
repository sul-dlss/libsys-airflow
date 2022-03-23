# frozen_string_literal: true

set :application, 'libsys-airflow'
set :repo_url, 'https://github.com/sul-dlss/libsys-airflow.git'
set :user, 'libsys'

# Default branch is :master
ask :branch, `git rev-parse --abbrev-ref HEAD`.chomp

# Default value for :log_level is :debug
set :log_level, :info

# Default deploy_to directory is /var/www/my_app_name
set :deploy_to, "/home/libsys/#{fetch(:application)}"

# Default value for linked_dirs is []
# set :linked_dirs, %w[]
set :linked_dirs, %w[cert]

# Default value for keep_releases is 5
set :keep_releases, 3

task :deploy do
  desc 'install dependencies'
  on roles(:app) do
    execute "cd #{release_path} && pip3 install -r requirements.txt"
  end
end
