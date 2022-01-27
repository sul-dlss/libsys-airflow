FROM apache/airflow:2.2.2-python3.9

USER root
RUN usermod -u 214 airflow

RUN apt-get -y update && apt-get -y install git

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/MARC21-To-FOLIO"

USER airflow
# RUN git clone -b sul-999-bib-locations https://github.com/sul-dlss/folio_migration.git migration --depth=2
COPY --chown=airflow migration migration/


RUN chmod +x migration/create_folder_structure.sh
RUN cd migration && rm -rf .git && ./create_folder_structure.sh


RUN git clone -b develop https://github.com/FOLIO-FSE/MARC21-To-FOLIO --depth=2

COPY bibs_transformer.py /opt/airflow/MARC21-To-FOLIO/migration_tools/migration_tasks/.
COPY holdings_marc_transformer.py /opt/airflow/MARC21-To-FOLIO/migration_tools/migration_tasks/.
COPY migration_task_base.py /opt/airflow/MARC21-To-FOLIO/migration_tools/migration_tasks/.
RUN cd MARC21-To-FOLIO && pip install -r requirements.txt
