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
glue_args = {
    'GlueVersion':"3.0", 
    'Timeout':3
    # Check why these are not working properly. It thinks I'm setting the AllocatedCapacity
    # This are needed to decrease cost, I think (2 vs. 6 DPUs)
    #'WorkerType': "G.1X",
    # 'NumberOfWorkers': 2, 
    }
aws_conn_id = 'aws_s3_default'

glue_job = AwsGlueJobOperator(job_name="reviews_transform_airflow",script_location=s3_path, create_job_kwargs = glue_args,
                                 iam_role_name=iam_role, region_name="us-east-2",task_id="glue_task", retry_limit=0,
                                 dag=dag, aws_conn_id=aws_conn_id, s3_bucket="s3://aws-glue-assets-921884731971-us-east-2/")

start_task = DummyOperator(task_id="start", dag=dag)

create_glue_job_task = PythonOperator (
    task_id='create_glue_job',
    python_callable=create_glue_job, 
    dag=dag
)

end_task   = DummyOperator(task_id="end", dag=dag)

start_task >> glue_job >> end_task