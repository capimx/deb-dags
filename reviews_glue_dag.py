import io
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
import airflow.utils.dates
import logging

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
import pandas as pd

# Operators; we need this to operate!
#from custom_modules.dag_s3_to_postgres import S3ToPostgresTransfer

default_args = {
    'owner': 'capimx',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1)
}

dag = DAG('reviews_etl', default_args = default_args, schedule_interval = '@daily')

def create_glue_job():
    return

s3_path = 's3://deb-capstone/reviews_transform.py'
iam_role  = "glue_job_role"
glue_args = {"MaxRetries" :  0,
            "WorkerType": "G.1X",
            "NumberOfWorkers": 2, 
            "Timeout":3,
            "GlueVersion":"3.0" }

glue_job = AwsGlueJobOperator(script_location=s3_path, script_args=glue_args,
                                 iam_role_name=iam_role, region_name="us-east-2",task_id="glue_task",
                                 dag=dag)

start_task = DummyOperator(task_id="start", dag=dag)

create_glue_job_task = PythonOperator (
    task_id='create_glue_job',
    python_callable=create_glue_job, 
    dag=dag
)

#create_table_task = PythonOperator() #ToDo finish this.

end_task   = DummyOperator(task_id="end", dag=dag)


start_task >> glue_job >> end_task