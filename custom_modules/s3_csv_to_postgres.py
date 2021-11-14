"""
S3 CSV to Postgres DB
"""

from airflow.exceptions import AirflowException
import airflow.utils.dates
from airflow.hooks.postgres_hook import PostgresHook
from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from datetime import datetime, timedelta


class S3CsvToPostgresOperator(BaseOperator):
    def __init__(
            self,            
            schema,
            table,
            s3_bucket,
            s3_key,
            aws_conn_postgres_id ='postgres_default',
            aws_conn_id='aws_default',
            verify=None,
            wildcard_match=False,
            copy_options=tuple(),
            autocommit=False,
            parameters=None, 
            **kwargs):
        super().__init__(**kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_conn_postgres_id  = aws_conn_postgres_id 
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.wildcard_match = wildcard_match
        self.copy_options = copy_options
        self.autocommit = autocommit
        self.parameters = parameters

    def execute(self, context):
        # Create instances for hooks        
        self.log.info(self.aws_conn_postgres_id)   
        self.pg_hook = PostgresHook(postgre_conn_id = self.aws_conn_postgres_id)
        self.s3 = S3Hook(aws_conn_id = self.aws_conn_id, verify = self.verify)

        # Locate file
        if self.wildcard_match:
            if not self.s3.check_for_wildcard_key(self.s3_key, self.s3_bucket):
                raise AirflowException("No key matches {0}".format(self.s3_key))
            s3_key_object = self.s3.get_wildcard_key(self.s3_key, self.s3_bucket)
        else:
            if not self.s3.check_for_key(self.s3_key, self.s3_bucket):
                raise AirflowException(
                    "The key {0} does not exists".format(self.s3_key))
                  
            s3_key_object = self.s3.get_key(self.s3_key, self.s3_bucket)


        message = f"Hello {self.name}"
        print(message)
        return message

