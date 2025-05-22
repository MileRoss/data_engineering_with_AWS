import pendulum
import datetime
import logging

from airflow.decorators import dag,task
from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

import sql_statements

@dag(
    description="Analyzes Divvy Bikeshare Data",
    start_date=pendulum.now(),
    end_date=pendulum.datetime(2026, 1, 1, 0, 0, 0, 0),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1
)
def s3_to_redshift_with_quality_check():

    @task(sla=datetime.timedelta(hours=1))
    def load_trip_data_to_redshift():
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection("aws_credentials")
        redshift_hook = PostgresHook("redshift")
        sql_stmt = sql_statements.COPY_ALL_TRIPS_SQL.format(
            aws_connection.login,
            aws_connection.password
        )
        redshift_hook.run(sql_stmt)

    @task()
    def load_station_data_to_redshift():
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection("aws_credentials")
        redshift_hook = PostgresHook("redshift")
        sql_stmt = sql_statements.COPY_STATIONS_SQL.format(
            aws_connection.login,
            aws_connection.password
        )
        redshift_hook.run(sql_stmt)

    @task()
    def check_greater_than_zero(**kwargs):
        table = kwargs["params"]["table"]
        redshift_hook = PostgresHook("redshift")
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {table} contained 0 rows")
        logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")


    create_trips_table = PostgresOperator(
        task_id="create_trips_table",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_TRIPS_TABLE_SQL
    )

    create_stations_table = PostgresOperator(
        task_id="create_stations_table",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_STATIONS_TABLE_SQL
    )

    location_traffic_drop = PostgresOperator(
    task_id="location_traffic_drop",
    postgres_conn_id="redshift",
    sql=sql_statements.LOCATION_TRAFFIC_SQL_DROP
    )

    location_traffic_create = PostgresOperator(
        task_id="location_traffic_create",
        postgres_conn_id="redshift",
        sql=sql_statements.LOCATION_TRAFFIC_SQL_CREATE
    )

    load_trips_task = load_trip_data_to_redshift()

    check_trips_task = check_greater_than_zero(
        params={"table":"trips"}
    )

    load_stations_task = load_station_data_to_redshift()

    check_stations_task = check_greater_than_zero(
        params={"table": "stations"}
    )

create_trips_table >> load_trips_task >> check_trips_task >> location_traffic_drop >> location_traffic_create
create_stations_table >> load_stations_task >> check_stations_task


s3_to_redshift_with_quality_check_dag = s3_to_redshift_with_quality_check()
