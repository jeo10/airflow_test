from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

@dag
def upload_to_minio():

     @task
     def upload():
         bucket_name = "airflow-test"
         object_name = "test.txt"
         hook = S3Hook(aws_conn_id='minio_s3')
         hook.load_string(
             string_data = "prueba de texto en archivo.",
             key = object_name,
             bucket_name = bucket_name,
             replace = True,
         )

     upload()

upload_to_minio()