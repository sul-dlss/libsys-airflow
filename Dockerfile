FROM apache/airflow:2.2.2-python3.9

USER root
RUN usermod -u 214 airflow

RUN apt-get -y update && apt-get -y install git

USER airflow

RUN git clone https://github.com/FOLIO-FSE/migration_repo_template migration
RUN chmod +x migration/create_folder_structure.sh
RUN cd migration && rm -rf .git && ./create_folder_structure.sh
RUN rm migration/data/instance/bib_example.mrc

RUN git clone https://github.com/FOLIO-FSE/MARC21-To-FOLIO --depth=2
RUN cd MARC21-To-FOLIO && pip install -r requirements.txt
