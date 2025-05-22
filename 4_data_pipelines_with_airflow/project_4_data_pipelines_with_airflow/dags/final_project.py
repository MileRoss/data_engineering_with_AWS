from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    "owner": "udacity",
    "depends_on_past": False,
    'start_date': pendulum.now(),
    "retries": 3,
    "retry_delay": timedelta(seconds=300),
    "catchup": False,
    "email_on_retry": False
}

@dag(
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval='@hourly'
)
def final_project():

    start_operator = EmptyOperator(task_id="Begin_execution")

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        table="staging_events",
        s3_key = "log_data/2018/11/",
        json_path="s3://udacity-dend/log_json_path.json",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        table="staging_songs",
        s3_key="song_data/",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials"
    )

    load_songplays_fact_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        redshift_conn_id="redshift",
        table="songplays",
        sql_statement=SqlQueries.songplay_table_insert
    )

    load_artist_dim_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        redshift_conn_id="redshift",
        table="artists",
        sql_statement=SqlQueries.artist_table_insert,
        insert_mode="truncate-insert"
    )

    load_song_dim_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        redshift_conn_id="redshift",
        table="songs",
        sql_statement=SqlQueries.song_table_insert,
        insert_mode="truncate-insert"
    )

    load_time_dim_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        redshift_conn_id="redshift",
        table="time",
        sql_statement=SqlQueries.time_table_insert,
        insert_mode="truncate-insert"
    )

    load_user_dim_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        redshift_conn_id="redshift",
        table="users",
        sql_statement=SqlQueries.user_table_insert,
        insert_mode="truncate-insert"
    )

    run_data_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        redshift_conn_id="redshift",
        test_cases=[
            {"check_sql": "SELECT COUNT(*) FROM songs WHERE songid IS NULL", "expected_result": 0}
        ]
    )
    
    end_operator = EmptyOperator(task_id="Stop_execution")

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_fact_table
    load_songplays_fact_table >> [load_user_dim_table, load_song_dim_table, load_artist_dim_table, load_time_dim_table]
    [load_user_dim_table, load_song_dim_table, load_artist_dim_table, load_time_dim_table] >> run_data_quality_checks >> end_operator


final_project_dag = final_project()
