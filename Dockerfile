FROM apache/airflow:2.2.2-python3.9

USER root
RUN usermod -u 214 airflow

RUN apt-get -y update && apt-get -y install git

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/folio_migration_tools"

USER airflow

# Once PR https://github.com/FOLIO-FSE/folio_migration_tools/pull/127 is merged
# switch repo to use main branch of https://github.com/FOLIO-FSE/folio_migration_tools/
RUN git clone -b fix-holdings-processor-init https://github.com/jermnelson/folio_migration_tools.git --depth=2

RUN cd folio_migration_tools && pip install -r requirements.txt && pip install -U folioclient
