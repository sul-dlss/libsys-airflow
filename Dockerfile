FROM apache/airflow:2.2.4-python3.9

USER root
RUN usermod -u 214 airflow

RUN apt-get -y update && apt-get -y install git

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/folio_migration_tools"

USER airflow

RUN git clone https://github.com/FOLIO-FSE/folio_migration_tools.git --depth=2

RUN cd folio_migration_tools && pip install -r requirements.txt
