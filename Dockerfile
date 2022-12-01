FROM apache/airflow:2.4.1-python3.10

USER root
RUN usermod -u 214 airflow
RUN apt-get update && apt-get install -y gcc
RUN apt-get -y update && apt-get -y install git

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/"
ENV SLUGIFY_USES_TEXT_UNIDECODE "yes"

USER airflow

COPY requirements.txt .

RUN pip install -r requirements.txt

RUN pip install apache-airflow
RUN pip install apache-airflow-providers-postgres
