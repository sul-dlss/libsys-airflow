FROM apache/airflow:2.2.2-python3.9

USER root
RUN usermod -u 214 airflow

RUN apt-get -y update && apt-get -y install git

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/MARC21-To-FOLIO"

USER airflow
COPY --chown=airflow:root migration migration/


RUN chmod +x migration/create_folder_structure.sh
RUN cd migration && rm -rf .git && ./create_folder_structure.sh

# Once PR https://github.com/FOLIO-FSE/folio_migration_tools/pull/125 is merged
# switch repo to use main branch of https://github.com/FOLIO-FSE/folio_migration_tools/
RUN git clone -b fix-holdings-processor-init https://github.com/jermnelson/MARC21-To-FOLIO.git --depth=2

RUN cd MARC21-To-FOLIO && pip install -r requirements.txt
