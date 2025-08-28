FROM apache/airflow:3.0.0
COPY my-sdk /opt/airflow/my-sdk
USER airflow
RUN pip install --no-cache-dir -e /opt/airflow/my-sdk apache-airflow[amazon]
USER root