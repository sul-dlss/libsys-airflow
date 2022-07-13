FROM apache/airflow:2.3.3-python3.10

USER root
RUN usermod -u 214 airflow
RUN apt-get update && apt-get install -y gcc

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/"
ENV SLUGIFY_USES_TEXT_UNIDECODE "yes"

USER airflow

COPY requirements.txt .

RUN pip install -r requirements.txt
# Needed to fix an import error see https://stackoverflow.com/questions/47884709/python-importerror-no-module-named-pluggy
RUN pip install -U pytest-metadata
# Install FOLIO-FSE tools
RUN pip install folioclient folio-migration-tools folio-uuid
