FROM apache/airflow:3.1.0-python3.10

USER root
RUN usermod -u 214 airflow
RUN apt-get update && apt-get install -y gcc git libmagic-dev

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/"
ENV SLUGIFY_USES_TEXT_UNIDECODE "yes"

COPY log_config.py ./config/log_config.py

USER airflow

COPY airflow.cfg requirements.txt pyproject.toml poetry.lock ./
COPY libsys_airflow ./libsys_airflow
COPY bin ./bin

RUN pip install -r requirements.txt
RUN poetry build --format=wheel --no-interaction --no-ansi
RUN pip install dist/*.whl