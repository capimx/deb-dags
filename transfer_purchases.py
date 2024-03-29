import airflow.utils.dates

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from custom_modules.s3_csv_to_postgres import S3CsvToPostgresOperator

default_args = {
    'owner': 'capimx',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1)
}

dag = DAG('transfer_purchases', default_args = default_args, schedule_interval = '@daily')

process_dag = S3CsvToPostgresOperator(
    task_id = 'dag_s3_csv_to_postgres',
    schema = 'bootcampdb',
    table= 'products',
    s3_bucket = 'deb-capstone',
    s3_key =  'user_purchase.csv',
    aws_conn_postgres_id = 'postgres_default',
    aws_conn_id = 'aws_s3_default',   
    dag = dag
)

process_dag