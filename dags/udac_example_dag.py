from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

#from airflow.operators import PostgresOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries
import configparser

config = configparser.ConfigParser()
config.read_file(open('dags/dwh.cfg'))

AWS_KEY = config.get('AWS','KEY')
AWS_SECRET = config.get('AWS','SECRET')

default_args = {
    'owner': 'ljia-ch',
    'denpends_on_past': False,
    'email':['ljia24338@gmail.com'],
    'email_on_failure': False,
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('sparkify_airflow_proj_dag',
          default_args=default_args,
          description='Load data from S3 and transform to Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data', 
    copy_json_option='s3://udacity-dend/log_json_path.json', # json path file for map JSON keys in COPY SQL
    region='us-west-2',
    dag=dag
    
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    copy_json_option='auto',
    region='us-west-2',
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id='redshift',
    table='songplays',
    query_name=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id='redshift',
    table='users',
    sql_name=SqlQueries.user_table_insert,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id='redshift',
    table='songs',
    sql_name=SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id='redshift',
    table='artists',
    sql_name=SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id='redshift',
    table='time',
    sql_name=SqlQueries.time_table_insert,
    dag=dag
)

run_song_quality_checks = DataQualityOperator(
    task_id='Run_song_data_quality_checks',
    redshift_conn_id='redshift',
    test_sql='select count(*) from songs where songid is null',
    expected_result=0,
    dag=dag
)

run_user_quality_checks = DataQualityOperator(
    task_id='Run_user_data_quality_checks',
    redshift_conn_id='redshift',
    test_sql='select count(*) from users where userid is null',
    expected_result=0,
    dag=dag
)

run_artist_quality_checks = DataQualityOperator(
    task_id='Run_artist_data_quality_checks',
    redshift_conn_id='redshift',
    test_sql='select count(*) from artists where artistid is null',
    expected_result=0,
    dag=dag
)

run_time_quality_checks = DataQualityOperator(
    task_id='Run_time_data_quality_checks',
    redshift_conn_id='redshift',
    test_sql='select count(*) from time where start_time is null',
    expected_result=0,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables

create_tables >> stage_events_to_redshift
create_tables >> stage_events_to_redshift

stage_events_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_user_quality_checks
load_song_dimension_table >> run_song_quality_checks
load_artist_dimension_table >> run_artist_quality_checks
load_time_dimension_table >> run_time_quality_checks

run_user_quality_checks >> end_operator 
run_song_quality_checks >> end_operator 
run_artist_quality_checks >> end_operator 
run_time_quality_checks >> end_operator 


