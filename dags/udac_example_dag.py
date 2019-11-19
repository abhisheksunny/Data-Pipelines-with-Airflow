from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
s3_bucket="s3://udacity-dend/"

default_args = {
    'owner': 'Abhishek Baranwal',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 1),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),    
}

dag = DAG(
    'udac_example_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    catchup=False,
    schedule_interval='@hourly'
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    creation_query=SqlQueries.staging_events_table_create,
    table_name="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_path=s3_bucket+"log_data/",
    json_data_format=s3_bucket+"log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    creation_query=SqlQueries.staging_songs_table_create,
    table_name="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_path=s3_bucket+"song_data/",
    json_data_format="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table_name="songplays",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    creation_query=SqlQueries.songplay_table_create,
    data_insertion_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table_name="users",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    creation_query=SqlQueries.user_table_create,
    data_insertion_query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table_name="songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    creation_query=SqlQueries.song_table_create,
    data_insertion_query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table_name="artists",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    creation_query=SqlQueries.artist_table_create,
    data_insertion_query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table_name="time",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    creation_query=SqlQueries.time_table_create,
    data_insertion_query=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    fact_tables=["songplays"],
    dim_tables=["users", "songs", "artists", "time"],
    staging_tables=["staging_events", "staging_songs"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

###############################################################################################

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator

#run_quality_checks >> start_operator
