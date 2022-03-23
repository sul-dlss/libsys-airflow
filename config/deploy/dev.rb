# frozen_string_literal: true

server 'sul-libsys-airflow-dev.stanford.edu', user: fetch(:user).to_s, roles: %w[app]

# allow ssh to host
Capistrano::OneTimeKey.generate_one_time_key!
