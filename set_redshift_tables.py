import io
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
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

dag = DAG('set_redshift_tables', default_args = default_args, schedule_interval = '@daily')

create_main_schema = """
drop schema if exists movies_schema;
create external schema movies_schema 
from data catalog 
database 'movies_db' 
iam_role 'arn:aws:iam::921884731971:role/redshift_role'
create external database if not exists;
"""

create_reviews_external = """
DROP TABLE IF EXISTS movies_schema.positive_reviews;
CREATE EXTERNAL TABLE movies_schema.positive_reviews (
  cid             varchar(20),
  positive_review int
)
STORED AS parquet
LOCATION 's3://deb-silver/positive_reviews/'
TABLE PROPERTIES ('skip.header.line.count'='1');
"""

create_purchases_external = """
DROP SCHEMA IF EXISTS movie_purchases;

CREATE EXTERNAL SCHEMA IF NOT EXISTS movie_purchases
FROM POSTGRES
DATABASE 'dbname'
URI 'recreated-purchases.ca29galgbvzk.us-east-2.rds.amazonaws.com'
IAM_ROLE 'arn:aws:iam::921884731971:role/redshift-secrets'
SECRET_ARN 'arn:aws:secretsmanager:us-east-2:921884731971:secret:deb/replica_pwd-3MalvV';
"""
user_behavior_insert_query="""
INSERT INTO user_behavior_metric(
  WITH reviews_view AS
          (	SELECT reviews.cid, SUM(reviews.positive_review) AS review_score, COUNT(reviews.positive_review) AS review_count
          	FROM movies_schema.positive_reviews AS reviews
          	WHERE reviews.cid IS NOT NULL
          	GROUP BY cid), 
      purchases_view AS
          (	SELECT purchases.customerid, SUM(purchases.quantity*purchases.unitprice) AS amount_spent
            FROM movie_purchases.user_purchases AS purchases
            WHERE purchases.customerid IS NOT NULL
            AND LEN(purchases.customerid) > 0
            AND purchases.quantity > 0
            AND purchases.unitprice > 0
            GROUP BY purchases.customerid)
  SELECT cid, amount_spent, review_score, review_count,  CURRENT_DATE
  FROM reviews_view AS rv
  JOIN purchases_view AS pv ON rv.cid = pv.customerid);
  """
def run_queries():
    
    task_id = 'run_queries'
    schema = 'bootcampdb'
    table= 'user_purchases'
    s3_bucket = 'deb-bronze'
    s3_key =  'user_purchase.csv'
    aws_conn_postgres_id = 'redshift_pg'

    # Create instances for hooks        
    logging.info(aws_conn_postgres_id)   
    pg_hook = PostgresHook(postgre_conn_id = aws_conn_postgres_id)

    # Locate file
    
    # Create table
    pg_hook.run(create_main_schema)
    #curr = pg_hook.get_conn().cursor()

    # 
 


start_task = DummyOperator(task_id="start", dag=dag)

locate_file_task = PythonOperator (
    task_id='locate_file',
    python_callable=locate_file, 
    dag=dag
)

#create_table_task = PythonOperator() #ToDo finish this.

end_task   = DummyOperator(task_id="end", dag=dag)


start_task >> locate_file_task >> end_task