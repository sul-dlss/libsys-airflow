FROM apache/airflow:2.3.3-python3.10

USER root
RUN usermod -u 214 airflow
RUN apt-get update && apt-get install -y gcc
RUN apt-get -y update && apt-get -y install git

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/"
ENV SLUGIFY_USES_TEXT_UNIDECODE "yes"

USER airflow

COPY requirements.txt .

RUN pip install -r requirements.txt
# Needed to fix an import error see https://stackoverflow.com/questions/47884709/python-importerror-no-module-named-pluggy
RUN pip install -U pytest-metadata
# Install FOLIO-FSE tools
RUN pip install folioclient folio-uuid

RUN git clone https://github.com/sul-dlss/folio_migration_tools.git --depth=2
RUN pip install /opt/airflow/folio_migration_tools
