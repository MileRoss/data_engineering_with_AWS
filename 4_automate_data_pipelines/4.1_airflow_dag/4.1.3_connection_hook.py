import pendulum
import logging

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook

@dag(
    description="Analyzes Divvy Bikeshare Data",
    start_date=pendulum.now(),
    end_date=pendulum.datetime(2026, 1, 1, 0, 0, 0, 0),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1
)
def list_keys():

    @task
    def list_s3_keys():
        hook = S3Hook(aws_conn_id="aws_credentials")
        bucket = Variable.get("s3_bucket")
        prefix = Variable.get("s3_prefix")
        logging.info(f"Listing Keys from {bucket}/{prefix}")
        keys = hook.list_keys(bucket, prefix=prefix)
        for key in keys:
            logging.info(f"- s3://{bucket}/{key}")
    
    list_s3_keys()


list_keys_dag = list_keys()
