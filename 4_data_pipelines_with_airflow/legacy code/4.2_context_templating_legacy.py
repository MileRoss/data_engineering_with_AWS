import pendulum
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook


def log_details_legacy(*args, **kwargs):
    logging.info(f"Execution date is {kwargs["ds"]}")
    logging.info(f"My run id is {kwargs["run_id"]}")
    previous_ds = kwargs.get("prev_start_date_success")
    if previous_ds:
        logging.info(f"My previous run was on {previous_ds}")
    next_ds = kwargs.get("data_interval_end")
    if next_ds:
        logging.info(f"My next run will be {next_ds}")

dag = DAG(
    dag_id="log_details_legacy",
    description="Analyzes Divvy Bikeshare Data",
    start_date=pendulum.now(),
    end_date=pendulum.datetime(2026, 1, 1, 0, 0, 0, 0),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1
)

list_task = PythonOperator(
    task_id="log_details",
    python_callable=log_details_legacy,
    provide_context=True,
    dag=dag
)
