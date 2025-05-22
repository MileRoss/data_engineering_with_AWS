import pendulum

from airflow import DAG
from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

import sql_statements

def load_trip_data_to_redshift(**kwargs):
    metastoreBackend = MetastoreBackend()
    aws_connection=metastoreBackend.get_connection("aws_credentials")
    redshift_hook = PostgresHook("redshift")
    execution_date = kwargs["execution_date"]
    sql_stmt = sql_statements.COPY_MONTHLY_TRIPS_SQL.format(
        aws_connection.login,
        aws_connection.password,
        year=execution_date.year,
        month=execution_date.month
    )
    redshift_hook.run(sql_stmt)

def load_station_data_to_redshift():
    metastoreBackend = MetastoreBackend()
    aws_connection=metastoreBackend.get_connection("aws_credentials")
    redshift_hook = PostgresHook("redshift")
    sql_stmt = sql_statements.COPY_STATIONS_SQL.format(
        aws_connection.login,
        aws_connection.password,
    )
    redshift_hook.run(sql_stmt)

dag = DAG(
    dag_id="data_partitioning_legacy",
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

load_trips_task = PythonOperator(
    task_id="load_trips_from_s3_to_redshift",
    dag=dag,
    python_callable=load_trip_data_to_redshift,
    provide_context=True,
)

create_stations_table = PostgresOperator(
    task_id="create_stations_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_STATIONS_TABLE_SQL,
)

load_stations_task = PythonOperator(
    task_id="load_stations_from_s3_to_redshift",
    dag=dag,
    python_callable=load_station_data_to_redshift,
)

create_trips_table >> load_trips_task
create_stations_table >> load_stations_task
