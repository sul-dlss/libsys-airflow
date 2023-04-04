[![Coverage Status](https://coveralls.io/repos/github/sul-dlss/libsys-airflow/badge.svg?branch=main)](https://coveralls.io/github/sul-dlss/libsys-airflow?branch=main)

# libsys-airflow
Airflow DAGS for libsys processes and migrating ILS data into FOLIO

## Dependency Management and Packaging
Run `pip install -r requirements.txt` to install the dependencies.

## Running Locally with Docker
Based on the documentation, [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html).

> **NOTE** Make sure there is enough RAM available locally for the
> docker daemon, we recommend at least 5GB.

1. Clone repository `git clone https://github.com/sul-dlss/libsys-airflow.git`
2. Run `git submodule init` and then `git submodule update` to populated submodule.
4. If it's commented out, uncomment the line `- ./dags:/opt/airflow/dags` in docker-compose.yaml (under `volumes`, under `x-airflow-common`).
5. Start up docker locally.
7. Create a `.env` file with the `AIRFLOW_UID` and `AIRFLOW_GROUP` values.
8. Run `docker-compose build` to build the customized airflow image. (Note: the `usermod` command may take a while to complete when running the build.)
9. Run `docker-compose up airflow-init` to initialize the Airflow the first time you deploy Airflow
10. Bring up airflow, `docker-compose up` to run the containers in the
   foreground, use `docker-compose up -d` to run as a daemon.
1. Access Airflow locally at http://localhost
1. Log into the worker container using `docker exec -it libsys-airflow_airflow-worker_1 /bin/bash` to view the raw work files.

### For FOLIO migration loads
1. In the Airflow UI under Admin > Connections, add `bib_path` with connection type `File (Path)`.
1. In the Airflow UI under Admin > Variables, import the `folio-dev-variables.json` file from [shared_configs](https://github.com/sul-dlss/shared_configs).

## Deploying
### Prerequisites
1. Install `pip3` with `apt install python3-pip`
1. Install python virtual enviroments: `apt install python3.8-venv`
1. Create the virtual envirnment in the home directory: `python3 -m venv virtual-env`
1. Install docker-compose in the virtual environment: `source virtual-env/bin/activate && pip3 install docker-compose`

### Tasks
1. List all the airflow tasks using `cap -AT airflow`
```
cap airflow:build          # run docker-compose build for airflow
cap airflow:init           # run docker-compose init for airflow
cap airflow:ps             # show running docker processes
cap airflow:restart        # restart airflow
cap airflow:start          # start airflow
cap airflow:stop           # stop and remove all running docker containers
cap airflow:stop_release   # stop old release and remove all old running docker containers
cap airflow:webserver      # restart webserver
```

### Do the first time you bring up Libsys-Airflow:
1. Log into the server, `ksu` and install `apt install python3.8-venv libpq-dev`
1. Exit `ksu` and install the pyton virtual environment under the libsys home directory:
   1. `cd && mkdir virtual-env`
   1. `python3 -m venv /home/libsys/virtual-env/`
1. In your local environment do `cap ${stage} deploy`
1. Follow the instructions for [shared_configs/libsys-airflow](https://github.com/sul-dlss/shared_configs/tree/libsys-airflow#readme)
1. On the server `source /home/libsys/virtual-env && pip install docker-compose==1.29.2`
1. In your local environment do `cap ${stage} deploy:install`
1. Visit https://sul-libsys-airflow-{stage}.stanford.edu and complete the remaining steps.

## For subsequent deploys
`cap ${stage} deploy deploy:restart`

This will stop and remove the docker images for the previous release and start up a new one.

### For Aeon and Lobbytrack API calls
1. In the Airflow UI under Admin > Variables, import the `aeon-variables.json` and the `lobbytrack-variables.json` files from [shared_configs](https://github.com/sul-dlss/shared_configs).

## FOLIO Plugin
All FOLIO related code should be in the `folio` plugin. When developing
code in the plugin, you'll need to restart the `airflow-webserver` container
by running `cap {stage} airflow:webserver` or ssh into the server and run `docker-compose restart airflow-webserver`
to see changes in the running Airflow environment.

## Running the DAGs to load Folio Inventory
### Optionally turn off archiving for bulk loading
```
echo $OKAPI_PASSWORD
ssh folio@$PG_DB_HOST
psql -h localhost -U okapi
alter system set archive_mode=off;
ksu
systemctl restart postgresql
```

The `optimistic_locking_management` DAG requires a Postgres Airflow
[connection](https://airflow.apache.org/docs/apache-airflow/stable/concepts/connections.html) with the host, login, and password fields matching the
database being used by Okapi.

## Testing
- Activate python virtual environment: `source virtual-env/bin/activate`
- Install the FOLIO-FSE tools: `pip install folio_migration_tools`
- Install Airflow: `pip install apache-airflow apache-airflow-providers-postgres`

Run the flake8 linter:
`flake8 dags/ plugins/`

Then, to run the test suite, use [pytest](https://docs.pytest.org/).
`pytest`

To see stdout or stderr add the `-rP` flag:
`pytest -rP`

To run with code coverage:
`coverage run -m pytest`

## Symphony Mount
MARC data to be converted will be mounted on the sul-libsys-airflow server under `/sirsi_prod` which is a mount of `/s/SUL/Dataload/Folio` on the Symphony server.
