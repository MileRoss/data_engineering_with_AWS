# Instructions
# Define a function that uses the python logger to log a function.
# The DAG should run daily.

import pendulum
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator


# Define the functions
def hello_world():
    logging.info("Hello World")


def addition():
    logging.info(f"2 + 2 = {2+2}")


def subtraction():
    logging.info(f"6 - 2 = {6-2}")


def division():
    logging.info(f"10 / 2 = {int(10/2)}")


# Create the DAG
dag = DAG(
    dag_id="task_dependencies_legacy",
    description="Analyzes Divvy Bikeshare Data",
    start_date=pendulum.now(),
    end_date=pendulum.datetime(2026, 1, 1, 0, 0, 0, 0),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1
)

# Create the tasks
hello_world_task = PythonOperator(
    task_id="hello_world",
    python_callable=hello_world,
    dag=dag)

# TODO: Define an addition task that calls the `addition` function above
addition_task = PythonOperator(
    task_id="addition",
    python_callable=addition,
    dag=dag)

# TODO: Define a subtraction task that calls the `subtraction` function above
subtraction_task = PythonOperator(
    task_id="subtraction",
    python_callable=subtraction,
    dag=dag)

# TODO: Define a division task that calls the `division` function above
division_task = PythonOperator(
    task_id="division",
    python_callable=division,
    dag=dag)

# TODO: Configure the task dependencies such that the graph looks like the following:
#
#                    ->  addition_task
#                   /                 \
#   hello_world_task                   -> division_task
#                   \                 /
#                    ->subtraction_task

hello_world_task >> addition_task
hello_world_task >> subtraction_task
subtraction_task >> division_task
