# frozen_string_literal: true

namespace :docker do
  task :ps do
    on roles(:app) do
      execute "cd #{release_path} && docker ps"
    end
  end

  task :build do
    on roles(:app) do
      execute "cd #{release_path} && docker-compose build"
    end
  end

  task :stop do
    on roles(:app) do
      execute "cd #{release_path}"
      execute "docker stop $(docker ps -a -q)"
      execute "docker rm $(docker ps -a -q)"
    end
  end

  task :start do
    on roles(:app) do
      execute "cd #{release_path}"
      execute "docker-compose build"
      execute "docker compose up airflow-init"
      execute "docker compose up -d"
    end
  end

  task :restart do
    on roles(:app) do
      stop
      start
    end
  end
end