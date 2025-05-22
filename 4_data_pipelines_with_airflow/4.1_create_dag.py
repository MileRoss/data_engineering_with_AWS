import pendulum
import logging
from airflow.decorators import dag, task

@dag(
    description="Analyzes Divvy Bikeshare Data",
    start_date=pendulum.now(),
    end_date=pendulum.datetime(2026, 1, 1, 0, 0, 0, 0),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1
)
def task_dependencies():

    @task()
    def hello_world():
        logging.info("Hello World")

    @task()
    def addition(first,second):
        logging.info(f"{first} + {second} = {first+second}")
        return first+second

    @task()
    def subtraction(first,second):
        logging.info(f"{first} - {second} = {first-second}")
        return first-second

    @task()
    def division(first,second):
        logging.info(f"{first} / {second} = {int(first/second)}")   
        return int(first/second)     
    
# TODO: call the hello world task function
# TODO: call the addition function with some constants (numbers)
# TODO: assign the result of the addition function to a variable
# TODO: call the subtraction function with some constants (numbers)
# TODO: assign the result of the subtraction function to a variable
# TODO: call the division function with the result of the addition and subtraction functions
# TODO: create the dependency graph like the following:
#
#                    ->  addition_task
#                   /                 \
#   hello_world_task                   -> division_task
#                   \                 /
#                    ->subtraction_task

    hello_world_task = hello_world()
    addition_task = addition(1,2)
    subtraction_task = subtraction(1,2)
    division_task = division(addition_task, subtraction_task)

    hello_world_task >> addition_task
    hello_world_task >> subtraction_task
    subtraction_task >> division_task


task_dependencies_dag = task_dependencies()
