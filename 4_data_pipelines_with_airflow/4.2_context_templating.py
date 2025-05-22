"""
TODO: Extract ds, run_id, prev_ds, and next_ds from the kwargs, and log them
NOTE: Look here for context variables passed in on kwargs:
https://airflow.apache.org/docs/apache-airflow/stable/macros-ref.html
"""

import pendulum
import logging

from airflow.decorators import dag, task
from airflow.models import Variable

@dag(
    description="Analyzes Divvy Bikeshare Data",
    start_date=pendulum.now(),
    end_date=pendulum.datetime(2026, 1, 1, 0, 0, 0, 0),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1
)
def log_details():

    @task
    def log_execution_date(**kwargs):
        logging.info(f"Execution date is {kwargs["ds"]}")

    @task
    def log_run_id(**kwargs):
        logging.info(f"My run id is {kwargs["run_id"]}")
    
    @task
    def log_previous_run(**kwargs):
        logging.info(f"My previous run was on {kwargs["prev_start_date_success"]}")
    
    @task
    def log_next_run(**kwargs):
        logging.info(f"My next run will be {kwargs["data_interval_end"]}")


    log_execution_date_task = log_execution_date()
    log_run_id_task = log_run_id()
    log_previous_run_task = log_previous_run()
    log_next_run_task = log_next_run()


log_details_dag = log_details()
