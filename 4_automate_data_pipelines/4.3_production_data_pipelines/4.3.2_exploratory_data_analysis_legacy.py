import datetime
import logging

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

import sql_statements

dag = DAG(
    dag_id="exploratory_data_analysis",
    description="Analyzes Divvy Bikeshare Data",
    start_date=pendulum.now(),
    end_date=pendulum.datetime(2026, 1, 1, 0, 0, 0, 0),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1
)

def log_younger_riders():
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""
        SELECT MIN(birthyear), MAX(birthyear) FROM young_riders
    """)
    if records and records[0]:
        logging.info(f"Riders born after 2000 range from {records[0][0]} to {records[0][1]}")

def log_youngest_rider():
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""
        SELECT birthyear FROM young_riders ORDER BY birthyear DESC LIMIT 1
    """)
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Youngest rider was born in {records[0][0]}")

def log_lifetime_rides():
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""
        SELECT bikeid FROM lifetime_rides ORDER BY ride_count DESC LIMIT 1
    """)
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"The bike with most rides is {records[0][0]}")

def log_city_station_counts():
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""
        SELECT city FROM city_station_counts ORDER BY station_count DESC LIMIT 1
    """)
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"The city with most station is {records[0][0]}")

def log_oldest_rider():
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""
        SELECT birthyear FROM older_riders ORDER BY birthyear ASC LIMIT 1
    """)
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Oldest rider was born in {records[0][0]}")


create_young_riders_task = PostgresOperator(
    task_id="create_young_riders",
    dag=dag,
    sql="""
        BEGIN;
        DROP TABLE IF EXISTS young_riders;
        CREATE TABLE young_riders AS (
        SELECT * FROM trips WHERE birthyear > 2000
        );
        COMMIT;
    """,
    postgres_conn_id="redshift"
)

create_lifetime_rides_task = PostgresOperator(
    task_id="create_lifetime_rides",
    dag=dag,
    sql="""
        BEGIN;
        DROP TABLE IF EXISTS lifetime_rides;
        CREATE TABLE lifetime_rides AS (
            SELECT bikeid, COUNT(bikeid) AS ride_count
            FROM trips
            GROUP BY bikeid
        );
        COMMIT;
    """,
    postgres_conn_id="redshift"
    )

create_city_station_counts_task = PostgresOperator(
    task_id="create_city_station_counts",
    dag=dag,
    sql="""
        BEGIN;
        DROP TABLE IF EXISTS city_station_counts;
        CREATE TABLE city_station_counts AS(
            SELECT city, COUNT(city) AS station_count
            FROM stations
            GROUP BY city
        );
        COMMIT;
    """,
    postgres_conn_id="redshift"
    )

create_oldest_rider_task = PostgresOperator(
    task_id="create_oldest_rider",
    dag=dag,
    sql="""
        BEGIN;
        DROP TABLE IF EXISTS older_riders;
        CREATE TABLE older_riders AS (
            SELECT * FROM trips WHERE birthyear > 0 AND birthyear <= 1945
        );
        COMMIT;
    """,
    postgres_conn_id="redshift"
)

log_younger_riders_task = PythonOperator(
    task_id="log_younger_riders",
    dag=dag,
    python_callable=log_younger_riders
)

log_youngest_rider_task = PythonOperator(
    task_id="log_youngest_rider",
    dag=dag,
    python_callable=log_youngest_rider
)

log_lifetime_rides_task = PythonOperator(
    task_id="log_lifetime_rides",
    dag=dag,
    python_callable=log_lifetime_rides
)

log_city_station_counts_task = PythonOperator(
    task_id="log_city_station_counts",
    dag=dag,
    python_callable=log_city_station_counts
)

log_oldest_rider_task = PythonOperator(
    task_id="log_oldest_rider",
    dag=dag,
    python_callable=log_oldest_rider
)

create_young_riders_task >> log_younger_riders_task
create_young_riders_task >> log_youngest_rider_task
create_lifetime_rides_task >> log_lifetime_rides_task
create_city_station_counts_task >> log_city_station_counts_task
create_oldest_rider_task >> log_oldest_rider_task
