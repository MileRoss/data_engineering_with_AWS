import pendulum

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

from custom_operators.s3_to_redshift import S3ToRedshiftOperator
from custom_operators.has_rows import HasRowsOperator
from custom_operators.facts_calculator import FactsCalculatorOperator

import sql_statements

dag = DAG(
    dag_id="custom_operators_legacy",
    description="Analyzes Divvy Bikeshare Data",
    start_date=pendulum.now(),
    end_date=pendulum.datetime(2026, 1, 1, 0, 0, 0, 0),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1
)

create_trips_table = PostgresOperator(
    task_id="create_trips_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TRIPS_TABLE_SQL
)

create_stations_table = PostgresOperator(
    task_id="create_stations_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_STATIONS_TABLE_SQL,
)

copy_trips_task = S3ToRedshiftOperator(
    task_id="load_trips_from_s3_to_redshift",
    dag=dag,
    table="trips",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="sean-murdock",
    s3_key="data-pipelines/divvy/partitioned/{execution_date.year}/{execution_date.month}/divvy_trips.csv"
)

copy_stations_task = S3ToRedshiftOperator(
    task_id="load_stations_from_s3_to_redshift",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="sean-murdock",
    s3_key="data-pipelines/divvy/unpartitioned/divvy_stations_2017.csv",
    table="stations"
)

check_trips_task = HasRowsOperator(
    task_id="count_trips",
    dag=dag,
    redshift_conn_id="redshift",
    table="trips"
)

check_stations_task = HasRowsOperator(
    task_id="count_stations",
    dag=dag,
    redshift_conn_id="redshift",
    table="stations"
)

calculate_facts_trips_task = FactsCalculatorOperator(
    task_id="calculate_facts_trips",
    redshift_conn_id="redshift",
    origin_table="trips",
    destination_table="trips_facts",
    fact_column="tripduration",
    groupby_column="bikeid"
)

calculate_facts_stations_task = FactsCalculatorOperator(
    task_id="calculate_facts_stations",
    redshift_conn_id="redshift",
    origin_table="stations",
    destination_table="stations_facts",
    fact_column="capacity",
    groupby_column="city"
)

create_trips_table >> copy_trips_task >> check_trips_task >> calculate_facts_trips_task
create_stations_table >> copy_stations_task >> check_stations_task >> calculate_facts_stations_task
