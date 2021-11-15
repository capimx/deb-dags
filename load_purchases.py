from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
import airflow.utils.dates
import logging

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
#from custom_modules.dag_s3_to_postgres import S3ToPostgresTransfer

default_args = {
    'owner': 'capimx',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1)
}

dag = DAG('load_purchases_new', default_args = default_args, schedule_interval = '@daily')


def locate_file():
    
    task_id = 'dag_s3_to_postgres'
    schema = 'bootcampdb'
    table= 'products'
    s3_bucket = 'deb-capstone'
    s3_key =  'user_purchase.csv'
    aws_conn_postgres_id = 'postgres_default'
    aws_conn_id = 'aws_s3_default'

    # Create instances for hooks        
    logging.info(aws_conn_postgres_id)   
    pg_hook = PostgresHook(postgre_conn_id = aws_conn_postgres_id)
    s3 = S3Hook(aws_conn_id = aws_conn_id, verify = None)

    # Locate file

    if not s3.check_for_key(s3_key, s3_bucket):
        
            logging.error("The key {0} does not exists".format(s3_key))
            
    s3_key_object = s3.get_key(s3_key, s3_bucket)

    logging.info("success")
    message = f"Hello"
    print(message)


start_task = DummyOperator(task_id="start", dag=dag)

locate_file_task = PythonOperator (
    task_id='locate_file',
    python_callable=locate_file, 
    dag=dag
)

end_task   = DummyOperator(task_id="end", dag=dag)


start_task >> locate_file_task >> end_task