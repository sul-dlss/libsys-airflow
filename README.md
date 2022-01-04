# folio-airflow
Airflow DAGS for migrating ILS data into FOLIO

## Running Locally with Docker
Based on the documentation, [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html). 

> **NOTE** Make sure there is enough RAM available locally for the 
> docker daemon, we recommend at least 5GB. 

1. Clone repository `git clone https://github.com/sul-dlss/folio-airflow.git`
1. If it's commented out, uncomment the line `- ./dags:/opt/airflow/dags` in docker-compose.yaml (under `volumes`, under `x-airflow-common`).
1. Run `docker compose up airflow-init` to initialize the Airflow
1. Bring up airflow, `docker compose up` to run the containers in the  
   foreground, use `docker compose up -d` to run as a daemon.
1. Access Airflow locally at http://localhost:8080

## Dependency Management and Packaging
We are using [poetry][POET] to better manage dependency updates. To install
[poetry][POET], run the following command in your shell:

`curl -sSL https://install.python-poetry.org | python3 -`

[POET]: https://python-poetry.org/
[PYTEST]: https://docs.pytest.org/

## Symphony Mount
MARC data to be converted will be mounted on the sul-folio-airflow server under `/sirsi_dev` which is a mount of `/s/SUL/Dataload/Folio` on the Symphony server.
