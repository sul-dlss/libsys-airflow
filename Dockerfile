FROM apache/airflow:2.2.4-python3.9

USER root
RUN usermod -u 214 airflow


RUN apt-get -y --allow-insecure-repositories update && apt-get -y install git

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/folio_migration_tools"

USER airflow

# When PR https://github.com/FOLIO-FSE/folio_migration_tools/pull/173 is merged
# revert back to using the FOLIO-FSE repository
# RUN git clone https://github.com/FOLIO-FSE/folio_migration_tools.git --depth=2
RUN git clone -b srs-marc https://github.com/jermnelson/folio_migration_tools.git --depth=2

COPY requirements.txt .

RUN pip install -r requirements.txt
