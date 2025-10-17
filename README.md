[![Coverage Status](https://coveralls.io/repos/github/sul-dlss/libsys-airflow/badge.svg?branch=main)](https://coveralls.io/github/sul-dlss/libsys-airflow?branch=main)

# libsys-airflow

Airflow DAGS for libsys processes and migrating ILS data into FOLIO

## Dependency Management and Packaging

To install the dependencies, run:

- `pip install -r requirements.txt`
- `poetry install`
- `brew install libmagic`, `sudo apt-get install -y libmagic-dev`, or equivalent

### Adding new dependencies

When adding a new dependency to the application, follow the `poetry add` documentation (https://python-poetry.org/docs/cli/#add) to ensure that the dependency is captured in `pyproject.toml`.

## Running Locally with Docker

Based on the documentation, [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html).

> **NOTE** Make sure there is enough RAM available locally for the
> docker daemon, we recommend at least 5GB.

1. Clone repository `git clone https://github.com/sul-dlss/libsys-airflow.git`
1. Start up docker locally.
1. Create a `.env` file with the `AIRFLOW_UID` and `AIRFLOW_GROUP` values. For local development these can usually be `AIRFLOW_UID=50000` and `AIRFLOW_GROUP=0`. (See [Airflow docs](https://airflow.apache.org/docs/apache-airflow/2.5.0/howto/docker-compose/index.html#setting-the-right-airflow-user) for more info.)
1. Add to the `.env` values for environment variables used by DAGs. (These are usually applied to VMs by puppet.)

- `AIRFLOW_VAR_OKAPI_URL`
- `AIRFLOW_VAR_FOLIO_URL`
- `AIRFLOW_VAR_FOLIO_USER`
- `AIRFLOW_VAR_FOLIO_PASSWORD`

  These environment variables must be prefixed with `AIRFLOW_VAR_` to be accessible to DAGs. (See [Airflow env var documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html#storing-variables-in-environment-variables and `docker-compose.yml`).) They can have placeholder values. The secrets are in vault, not prefixed by `AIRFLOW_VAR_`: `vault kv list puppet/application/libsys_airflow/{env}`.

  Example script to quickly populate your .env file for dev:
  ```
  for i in `vault kv list puppet/application/libsys_airflow/dev`; do val=$(echo $i| tr '[a-z]' '[A-Z]'); echo AIRFLOW_VAR_$val=`vault kv get -field=content puppet/application/libsys_airflow/dev/$i`; done
  ```

  **NOTE** In order to connect to the OKAPI_URL you must be connected to the VPN or the on-campus network.

6. Add a `.aws` directory with the config and credentials for the S3 bucket your want to use.
  Example credentials
  ```
  [default]
  aws_access_key_id = myaccess
  aws_secret_access_key = mysecret
  ```

7. Run `docker compose build` to build the customized Airflow image. (Note: the `usermod` command may take a while to complete when running the build.)
8. Run `docker compose up airflow-init` to initialize the Airflow database and create a user the first time you deploy Airflow.
9. Bring up Airflow, `docker compose up` to run the containers in the foreground. Use `docker compose up -d` to run as a daemon.
10. Access Airflow locally at http://localhost:8080. The default username and password are both `airflow`.
11. Log into the worker container using `docker exec -it libsys-airflow-airflow-worker-1 /bin/bash` to view the raw work files.

## Deploying

### Prerequisites

1. Install `pip3` with `apt install python3-pip`
1. Install python virtual enviroments: `apt install python3.8-venv`
1. Install dependencies per `Dependency Management and Packaging` above
1. Install docker-compose in the poetry virtual environment: `poetry shell && pip3 install docker-compose`

### Tasks

List all the airflow tasks using `cap -AT airflow`

```
cap airflow:build          # run docker compose build for airflow
cap airflow:init           # run docker compose init for airflow
cap airflow:ps             # show running docker processes
cap airflow:restart        # restart airflow
cap airflow:start          # start airflow
cap airflow:stop           # stop and remove all running docker containers
cap airflow:stop_release   # stop old release and remove all old running docker containers
cap airflow:webserver      # restart webserver
```

#### Setup the list of databases for the database setup and migration tasks run via a capistrano deploy
List all the Alembic database migration tasks (see `Database migrations` below for more) using `cap -AT alembic`:

```
cap alembic:current  # Show current Alembic database migration
cap alembic:history  # Show Alembic database migration history
cap alembic:migrate  # Run Alembic database migrations
```

In `config/deploy.rb` at the top, add new default database set to the list of alembic_dbs:
```
set :alembic_dbs, ['vma', 'digital_bookplates', 'new_database', '...']
```

Or alternately, run any alembic task with the ALEMBIC_DBS environment variable set, e.g.
```
ALEMBIC_DBS='new_database, new_database2' cap dev alembic:migrate
```


### Do the first time you bring up Libsys-Airflow:

1. Log into the server, and run:
   1. `ksu`
   1. `apt install python3.11-venv libpq-dev`
   1. `cd /home/libsys`
   1. `python3.11 -m venv virtual-env`
   1. `touch /home/libsys/libsys-airflow/shared/config/.env`
   1. `vim /home/libsys/libsys-airflow/shared/config/.env` and add the following content:
      ```
      AIRFLOW_UID=214
      AIRFLOW_GROUP=0
      ```
1. In your local environment do `cap ${env} deploy deploy:install`
1. Visit https://sul-libsys-airflow-{env}.stanford.edu and complete the remaining steps. See shared_configs for instructions on getting the airflow admin user's password from vault.

## For subsequent deploys

`cap ${env} deploy`

This will stop and remove the docker images for the previous release and start up a new one.

## FOLIO Plugin

All FOLIO related code should be in the `folio` plugin. When developing
code in the plugin, you'll need to restart the `airflow-webserver` container
by running `cap {env} airflow:webserver` or ssh into the server and run `docker compose restart airflow-webserver`
to see changes in the running Airflow environment.

## Development

### Support for Multiple Databases with Alembic
We are supporting multiple databases, Vendor Management App and Digital Bookplates, using alembic following
these [directions](https://alembic.sqlalchemy.org/en/latest/cookbook.html#run-multiple-alembic-environments-from-one-ini-file).

To run any `alembic` commands you need add the `--name` parameter followed by `vma` for the Vendor Management database
and `digital_bookplates` for the Digital Bookplates database.

### Local Database Creation and Migrations
How to get the digital_bookplates database running locally starting with fresh postgres volume:

1. Bring up airflow: `docker compose up`
1. In a different terminal window, set these variables:
```
export DATABASE_USERNAME="airflow"
export DATABASE_PASSWORD="airflow"
export DATABASE_HOSTNAME="localhost"
```
1.  Run (e.g. using the database name you want to migrate) `poetry run alembic --name digital_bookplates upgrade head`

#### Clearing Digital Bookplates Data

Assuming that you have populated the digital_bookplates database's digital_bookplates table with data, you can clear it out with:
```
poetry run bin/truncate_digital_bookplates
```

In a deployed environment, this can be run as:
```
docker exec -it 20230516183735-airflow-webserver-1 bin/truncate_digital_bookplates
```
where `20230516183735-airflow-webserver-1` is the container id.

### Vendor load plugin

Using and developing the vendor load plug in requires its own database. Ensure that the `vendor_loads` database exists in your local postgres and is owned by the airflow user.

To access the database in development, install
(e.g. `brew install postgresql`) and from your local terminal, run:

```
psql -h localhost -U airflow
```

To use psql in the docker container:

```
docker exec -it libsys-airflow-postgres-1 psql -U airflow
```

#### Database migrations

Using [Alembic](https://alembic.sqlalchemy.org/en/latest/) to manage database migrations for the `vendor_loads` database, changes to the
Models are autogenerated in a migration file in the `vendor_loads_migration/versions` directory. The Alembic migration requires the following environmental variables to be set either through a local `.env` file or
injected when using docker-compose:

- **DATABASE_USERNAME**
- **DATABASE_PASSWORD**
- **DATABASE_HOSTNAME**

When running locally you can use the `dotenv` command installed with the `python-dotenv` requirement to automatically put your `.env` variables into the environment.

To generate a migration script, first make the changes in the `models.py`
module and then run the following steps:

1. Set your shell to use the local poetry virtual environment: `poetry shell`
2. Run `dotenv run alembic --name vma revision --autogenerate -m "{short message describing change}"` (**NOTE**: not all changes to the model are detected, see this [note](https://alembic.sqlalchemy.org/en/latest/autogenerate.html#what-does-autogenerate-detect-and-what-does-it-not-detect) in the documentation)
3. After the migration script is created, run `dotenv run alembic upgrade head` to apply your latest changes to the database.

If you prefer not to use `poetry shell` you can use `poetry run` along with `dotenv` instead: e.g. `poetry run dotenv run alembic --name vma upgrade head`. Or you can simply put the `DATABASE_*` environment variables into your shell via another means.

To fix multiple heads: `poetry run alembic --name vma merge heads -m "merge <revision 1> and <revision 2>"`

#### Seeding Vendors

Assuming that you have put Folio related environment variables in your `.env` file you can load all organizations (over 3,000) with:

```
dotenv run bin/seed_vendors
```

To load a limited set of only six vendors, use `--limited`:

```
dotenv run bin/seed_vendors --limited
```

In a deployed environment, the full load can be run as:
```
docker exec -it libsys_airflow-airflow-webserver-1 bin/seed_vendors
```

#### Refreshing Vendor Data

Assuming that you have put Folio related environment variables in your `.env` file you can look up acquisitions unit names in Folio and populate the database:
```
dotenv run bin/refresh_vendors
```

This can be run as a dry run with `-d` or `--dry`.

In a deployed environment, this can be run as:
```
docker exec -it 20230516183735-airflow-webserver-1 bin/refresh_vendors
```
where `20230516183735-airflow-webserver-1` is the container id.

#### Poetry Lock Merge Conflicts
If when doing a rebase you encounter a merge conflict with the `poetry.lock` file and assuming you have added/updated
dependencies to the `pyproject.toml`, run the following commands:

- `git checkout --theirs poetry.lock`
- `poetry lock --no-update`

## Testing

1. Install dependencies per `Dependency Management and Packaging` above
1. Drop into the poetry virtual environment: `poetry shell` (alternatively, if you don't want to drop into `poetry shell`, you can run commands using `poetry run my_cmd`, akin to `bundle exec my_cmd`)

Install the test database (sqlite):
`airflow db init`

Then, to run the test suite, use [pytest](https://docs.pytest.org/).
`poetry run pytest`

To see stdout or stderr add the `-rP` flag:
`poetry run pytest -rP`

### Flake8 (Python linter)

Flake8 configuration is in `setup.cfg` (flake8 cannot be configured with `pyproject.toml`).

For CI, see the `Lint with flake8` step in `.github/workflows/lint.yml`.

To run the flake8 linter: `poetry run flake8`

### Black (Python formatter)

Black configuration is in `pyproject.toml`.

For CI, see the `Format with black` step in `.github/workflows/lint.yml` github action. It has been configured to fail if there are any violations.

To run the black formatter to fix any violations:

`poetry run black .`

### Mypy (Python type checker)

[Mypy](https://github.com/python/mypy) is a static type checker.  Configuration is in `pyproject.toml`.

To run the mypy type checker on the codebase: `poetry run mypy libsys_airflow tests`.

The type checker will complain if assignments, return types, parameter types, etc are inconsistently used, e.g. if an int is provided for a string param, a function doesn't return the type it claims to, etc (helpful for documenting function signatures correctly, and for sussing out issues like inconsistent types for variable assignment or function return, which can be confusing at best, and bug prone at worst).

Type declarations are only used in testing and type checking, and do not affect runtime behavior.

If you run into something where the type checker complains and you're sure that the usage is ok, you can comment the line with `# type: ignore` to quiet the type checker for that line.  As with linting, use your best judgement as to whether an exception is preferable to mollifying the checker.

The type checker is not currently wired up in CI as a required check, but you may still find it useful for catching lurking consistency issues.

## Connections

### SFTP

SFTP connections may have a password or a key file. The key files should be named with the hostname (e.g., `sftp.amalivre.fr`) and placed in `vendor-keys/` directory.

## DAGs

### data_fetcher

* Use the `filename_regex` value `CNT-ORD` for special Gobi file filtering (filter .ord files that don't have a corresponding .cnt file).
* Set the `download_days_ago` airflow variable to limit downloads to a specific time period (default is 10 days).

## Cleanup of Logs and Workfiles

Edit `plugins/remove_old_files_cron.py` to set the days and times to execute the removal of old files. Set the `mtime` flag to the maximum number of days for file retention.

On the deployment server run `cap {stage} deploy write_crontab` or on the airflow server from within the current project directory run `crontab -r` and then `python plugins/remove_old_files_cron.py`.
