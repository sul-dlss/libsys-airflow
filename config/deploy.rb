# frozen_string_literal: true

set :application, 'libsys-airflow'
set :repo_url, 'https://github.com/sul-dlss/libsys-airflow.git'
set :user, 'libsys'
set :venv, '/home/libsys/virtual-env/bin/activate'
set :migration, 'https://github.com/sul-dlss/folio_migration.git'
set :alembic_dbs, ['vma', 'digital_bookplates']
set :ssh_options, { :forward_agent => true }

def alembic_dbs
  ENV['ALEMBIC_DBS']&.gsub(/\s/, '')&.split(',') || fetch(:alembic_dbs)
end

# Default branch is :main
ask :branch, `git rev-parse --abbrev-ref HEAD`.chomp

# Default value for :log_level is :debug
set :log_level, :info

# Default deploy_to directory is /var/www/my_app_name
set :deploy_to, "/home/libsys/#{fetch(:application)}"

# Default value for linked_dirs is []
# set :linked_dirs, %w[]
set :linked_dirs, %w(.aws config vendor-data vendor-keys data-export-files digital-bookplates fix_encumbrances orafin-files logs authorities)

# Default value for keep_releases is 5
set :keep_releases, 2

before 'deploy:cleanup', 'fix_permissions'
before 'deploy:published', 'deploy:restart'
after 'deploy:finishing', 'honeybadger:notify'
after 'deploy:finishing_rollback', 'honeybadger:notify'

desc 'Change release directories ownership'
task :fix_permissions do
  on roles(:app) do
    within releases_path do
      execute :sudo, :chown, '-R', "#{fetch(:user)}:#{fetch(:user)}", '*'
    end
  end
end

desc 'Install the crontab that removes logs older than 1 year'
task :write_crontab do
  on roles(:app) do
    execute "pip install python-crontab"
    execute "crontab -r; true" # clear out any existing crontab
    execute "python3 #{release_path}/plugins/remove_old_files_cron.py"
    execute "crontab -l"
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

namespace :db do
desc 'Create needed databases'
  task :create do
    ['airflow'].concat(alembic_dbs).each do |database|
      on roles(:app) do
        execute :psql, <<~PSQL_ARGS
          -v ON_ERROR_STOP=1 postgresql://$DATABASE_USERNAME:$DATABASE_PASSWORD@$DATABASE_HOSTNAME <<-SQL
          SELECT 'CREATE DATABASE #{database}' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '#{database}')\\gexec
          GRANT ALL PRIVILEGES ON DATABASE #{database} TO $DATABASE_USERNAME;
  SQL
      PSQL_ARGS
      end
    end
  end
end

namespace :alembic do
  desc 'Run Alembic database migrations'
  task :migrate do
    alembic_dbs.each do |database|
      on roles(:app) do
        execute "cd #{release_path} && source #{fetch(:venv)} && poetry run alembic --name #{database} upgrade head"
      end
    end
  end

  desc 'Show current Alembic database migration'
  task :current do
    alembic_dbs.each do |database|
      on roles(:app) do
        execute "cd #{release_path} && source #{fetch(:venv)} && echo #{database.upcase} && poetry run alembic --name #{database} current"
      end
    end
  end

  desc 'Show Alembic database migration history'
  task :history do
    alembic_dbs.each do |database|
      on roles(:app) do
        execute "cd #{release_path} && source #{fetch(:venv)} && poetry run alembic --name #{database} history --verbose"
      end
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

  desc 'run docker compose build for airflow'
  task :build do
    on roles(:app) do
      execute "cd #{release_path} && source #{fetch(:venv)} && docker compose -f compose.prod.yaml -p libsys_airflow build"
    end
  end

  desc 'stop and remove all running docker containers'
  task :stop do
    on roles(:app) do
      execute "cd #{release_path} && source #{fetch(:venv)} && docker compose -f compose.prod.yaml -p libsys_airflow stop"
    end
  end

  desc 'run docker compose init for airflow'
  task :init do
    on roles(:app) do
      execute "cd #{release_path} && source #{fetch(:venv)} && docker compose -f compose.prod.yaml -p libsys_airflow up airflow-init"
    end
  end

  desc 'start airflow'
  task :start do
    on roles(:app) do
      invoke 'airflow:build'
      invoke 'airflow:init'
      execute "cd #{release_path} && source #{fetch(:venv)} && docker compose -f compose.prod.yaml -p libsys_airflow up -d"
      invoke 'db:create'
      invoke 'alembic:migrate'
    end
  end

  desc 'restart webserver'
  task :webserver do
    on roles(:app) do
      execute "cd #{release_path} && source #{fetch(:venv)} && docker compose -f compose.prod.yaml -p libsys_airflow restart airflow-webserver"
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
      execute "cd #{release_path} && releases=($(ls -tr ../.)) && cd ../${releases[0]} && source #{fetch(:venv)} && docker compose -f compose.prod.yaml -p libsys_airflow stop"
      execute "[[ $(docker ps -aq) ]] && docker ps -aq | xargs docker stop | xargs docker rm || echo 'no containers to stop'"
    end
  end
end

namespace :honeybadger do
  desc 'Notify Honeybadger of a deploy (using the API via curl)'
  task notify: %i[deploy:set_current_revision] do
    on roles(:app) do
      info 'Notifying Honeybadger of deploy.'
      remote_api_key = capture(:echo, '$HONEYBADGER_API_KEY')
      remote_env = capture(:echo, '$HONEYBADGER_ENV')
      options = {
        'deploy[environment]' => remote_env,
        'deploy[local_username]' => fetch(:honeybadger_user, ENV['USER'] || ENV.fetch('USERNAME', nil)),
        'deploy[revision]' => fetch(:current_revision),
        'deploy[repository]' => fetch(:repo_url),
        'api_key' => remote_api_key
      }
      data = options.to_a.map { |pair| pair.join('=') }.join('&')
      execute(:curl, '--no-progress-meter', '--data', "\"#{data}\"", 'https://api.honeybadger.io/v1/deploys')
      info 'Honeybadger notification complete.'
    end
  end
end
