# frozen_string_literal: true

set :application, 'libsys-airflow'
set :repo_url, 'https://github.com/sul-dlss/libsys-airflow.git'
set :user, 'libsys'
set :venv, '/home/libsys/virtual-env/bin/activate'
set :migration, 'https://github.com/sul-dlss/folio_migration.git'

# Default branch is :main
ask :branch, `git rev-parse --abbrev-ref HEAD`.chomp

# Default value for :log_level is :debug
set :log_level, :info

# Default deploy_to directory is /var/www/my_app_name
set :deploy_to, "/home/libsys/#{fetch(:application)}"

# Default value for linked_dirs is []
# set :linked_dirs, %w[]
set :linked_dirs, %w[config]

# Default value for keep_releases is 5
set :keep_releases, 2

before 'deploy:cleanup', 'fix_permissions'

desc 'Change release directories ownership'
task :fix_permissions do
  on roles(:app) do
    within releases_path do
      execute :sudo, :chown, '-R', "#{fetch(:user)}:#{fetch(:user)}", '*'
    end
  end
end

namespace :deploy do
  desc 'deploy airflow when an instance is not currently running'
  task :install do
    on roles(:app) do
      invoke 'airflow:install'
      invoke 'airflow:start'
    end
  end

  desc 'deploy airflow when an instance is currently running'
  task :restart do
    on roles(:app) do
      invoke 'airflow:stop_release'
      invoke 'airflow:install'
      invoke 'airflow:start'
    end
  end
end

namespace :airflow do
  desc 'install airflow dependencies'
  task :install do
    on roles(:app) do
      execute "cd #{release_path} && source #{fetch(:venv)} && pip3 install -r requirements.txt && poetry build --format=wheel --no-interaction --no-ansi && pip3 install dist/*.whl"
      execute "cp #{release_path}/config/.env #{release_path}/."
      execute "cd #{release_path} && git clone #{fetch(:migration)} migration"
      execute "chmod +x #{release_path}/migration/create_folder_structure.sh"
      execute "cd #{release_path}/migration && ./create_folder_structure.sh"
      execute "cd #{release_path}/migration && mkdir -p archive"
      execute "cd #{release_path} && sudo ln -s /sirsi_prod symphony"
    end
  end

  desc 'send docker command'
  task :docker, :command do |task, args|
    on roles(:app) do
      execute "cd #{release_path} && source #{fetch(:venv)} && docker #{args[:command]}"
    end
  end

  desc 'show running docker processes'
  task :ps do
    on roles(:app) do
      execute "cd #{release_path} && source #{fetch(:venv)} && docker ps"
    end
  end

  desc 'run docker-compose build for airflow'
  task :build do
    on roles(:app) do
      execute "cd #{release_path} && source #{fetch(:venv)} && docker-compose -f docker-compose.prod.yaml build"
    end
  end

  desc 'stop and remove all running docker containers'
  task :stop do
    on roles(:app) do
      execute "cd #{release_path} && source #{fetch(:venv)} && docker-compose -f docker-compose.prod.yaml stop"
    end
  end

  desc 'run docker-compose init for airflow'
  task :init do
    on roles(:app) do
      execute "cd #{release_path} && source #{fetch(:venv)} && docker-compose -f docker-compose.prod.yaml up airflow-init"
    end
  end

  desc 'start airflow'
  task :start do
    on roles(:app) do
      invoke 'airflow:build'
      invoke 'airflow:init'
      execute "cd #{release_path} && source #{fetch(:venv)} && docker-compose -f docker-compose.prod.yaml up -d"
    end
  end

  desc 'restart webserver'
  task :webserver do
    on roles(:app) do
      execute "cd #{release_path} && source #{fetch(:venv)} && docker-compose -f docker-compose.prod.yaml restart airflow-webserver"
    end
  end

  desc 'restart airflow'
  task :restart do
    on roles(:app) do
      invoke 'airflow:stop'
      invoke 'airflow:start'
    end
  end

  desc 'stop old release and remove all old running docker containers'
  task :stop_release do
    on roles(:app) do
      execute "docker image prune -f"
      execute "cd #{release_path} && releases=($(ls -tr ../.)) && cd ../${releases[0]} && source #{fetch(:venv)} && docker-compose -f docker-compose.prod.yaml stop"
      execute "[[ $(docker ps -aq) ]] && docker ps -aq | xargs docker stop | xargs docker rm || echo 'no containers to stop'"
    end
  end

  desc 'list the contents of the symphony folder'
  task :symphony do
    on roles(:app) do
      execute "ls -ltr #{release_path}/symphony/"
    end
  end
end
