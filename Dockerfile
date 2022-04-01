FROM apache/airflow:2.2.4-python3.9

USER root
RUN usermod -u 214 airflow

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/"

USER airflow

COPY requirements.txt .

RUN pip install --no-deps -r requirements.txt
