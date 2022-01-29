FROM apache/airflow:2.2.2-python3.9

USER root
RUN usermod -u 214 airflow

RUN apt-get -y update && apt-get -y install git

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/MARC21-To-FOLIO"

USER airflow
COPY --chown=airflow migration migration/


RUN chmod +x migration/create_folder_structure.sh
RUN cd migration && rm -rf .git && ./create_folder_structure.sh


RUN git clone -b optional-logging https://github.com/jermnelson/MARC21-To-FOLIO --depth=2

RUN cd MARC21-To-FOLIO && pip install -r requirements.txt
