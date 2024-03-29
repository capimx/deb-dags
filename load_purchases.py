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

dag = DAG('load_purchases', default_args = default_args, schedule_interval = '@daily')
aws_conn_postgres_id = 'postgres_default'

create_table_cmd = """
DROP TABLE IF EXISTS user_purchases;
CREATE TABLE IF NOT EXISTS user_purchases (
   purchaseId SERIAL PRIMARY KEY,
   invoiceNo VARCHAR(10),
   StockCode VARCHAR(20),
   Description VARCHAR(70),
   Quantity INT,
   InvoiceDate TIMESTAMP,
   UnitPrice NUMERIC,
   CustomerID VARCHAR(10),
   Country VARCHAR(30)
);"""


create_materialized_view = """
DROP MATERIALIZED VIEW IF EXISTS valid_purchases;
CREATE MATERIALIZED VIEW valid_purchases
AS 
  SELECT quantity, unitprice, customerid FROM user_purchases
  WHERE customerid IS NOT NULL 
  AND length(customerid) > 0
  AND quantity  > 0
  AND unitprice > 0
  WITH DATA;
"""

def load_data():
    
    task_id = 'dag_s3_to_postgres'
    schema = 'bootcampdb'
    table= 'user_purchases'
    s3_bucket = 'deb-bronze'
    s3_key =  'user_purchase.csv'    
    aws_conn_id = 'aws_s3_default'

    # Create instances for hooks        
    logging.info(aws_conn_postgres_id)   
    pg_hook = PostgresHook(postgre_conn_id = aws_conn_postgres_id)
    s3 = S3Hook(aws_conn_id = aws_conn_id, verify = None)

    # Locate file

    if not s3.check_for_key(s3_key, s3_bucket):
        
            logging.error("The key {0} does not exist".format(s3_key))
            
    s3_key_object = s3.get_key(s3_key, s3_bucket)
    
    # Create table
    pg_hook.run(create_table_cmd)
    #curr = pg_hook.get_conn().cursor()

    # 
    file_content = s3_key_object.get()['Body'].read().decode(encoding = "utf-8", errors = "ignore")
  
    list_target_fields = [    'InvoiceNo', 
                              'StockCode',
                              'Description', 
                              'Quantity', 
                              'InvoiceDate', 
                              'UnitPrice', 
                              'CustomerID', 
                              'Country'
                              ]
    # schema definition for data types of the source.
    schema = {
                'InvoiceNo': 'string',
                'StockCode': 'string',
                'Description': 'string',
                'Quantity': 'string',
                'InvoiceDate': 'string',
                'UnitPrice': 'float64',                                
                'CustomerID': 'string',
                'Country': 'string'
                }  
    date_cols = ['fechaRegistro']         

    # read a csv file with the properties required.
    df_products = pd.read_csv(io.StringIO(file_content), 
                        header=0, 
                        delimiter=",",
                        quotechar='"',
                        low_memory=False,
                        #parse_dates=date_cols,                                             
                        dtype=schema                         
                        )
    # Reformat df
    df_products = df_products.replace(r"[\"]", r"'")
    df_products['CustomerID'] = df_products['CustomerID'].fillna("")
    df_products['Description'] = df_products['Description'].fillna("")
    list_df_products = df_products.values.tolist()
    list_df_products = [tuple(x) for x in list_df_products]
    current_table = "user_purchases"

    #Insert rows
    pg_hook.insert_rows(current_table,  
                                list_df_products, 
                                target_fields = list_target_fields, 
                                commit_every = 1000,
                                replace = False) 
   
    print("Finish")   


def create_mat_view():
    pg_hook = PostgresHook(postgre_conn_id = aws_conn_postgres_id)
    pg_hook.run(create_materialized_view)
    return


start_task = DummyOperator(task_id="start", dag=dag)

load_data_task = PythonOperator (
    task_id='load_data',
    python_callable=load_data, 
    dag=dag
)

create_mat_view_task = PythonOperator (
    task_id='create_mat_view',
    python_callable=create_mat_view, 
    dag=dag
)


#create_table_task = PythonOperator() #ToDo finish this.

end_task   = DummyOperator(task_id="end", dag=dag)


start_task >> load_data_task >> create_mat_view_task >> end_task