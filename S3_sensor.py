"""
S3 Sensor Connection Test
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'capimx',    
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('s3_sensor', default_args=default_args, schedule_interval= '@once')

t1 = BashOperator(
    task_id='bash_test',
    bash_command='echo "hello, it should work" > s3_conn_test.txt',
    dag=dag)

sensor = S3KeySensor(
    task_id='check_s3_for_file_in_s3',
    bucket_key='file-to-watch-*',
    wildcard_match=True,
    bucket_name='s3-delete-this',
    s3_conn_id='aws_s3_default',
    timeout=18*60*60,
    poke_interval=120,
    dag=dag)

t1.set_upstream(sensor)