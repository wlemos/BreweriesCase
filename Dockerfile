FROM apache/airflow:2.5.0 

USER airflow
RUN pip install s3fs
USER airflow
