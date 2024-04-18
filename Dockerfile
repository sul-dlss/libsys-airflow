FROM apache/airflow:2.8.3-python3.11

USER root
RUN usermod -u 214 airflow
RUN apt-get update && apt-get install -y gcc git libmagic-dev

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/"
ENV SLUGIFY_USES_TEXT_UNIDECODE "yes"

USER airflow

COPY airflow.cfg requirements.txt pyproject.toml qa.sql poetry.lock ./
COPY log_config.py ./config/log_config.py
COPY libsys_airflow ./libsys_airflow
COPY bin ./bin

RUN pip install -r requirements.txt
RUN poetry build --format=wheel --no-interaction --no-ansi
RUN pip install dist/*.whl