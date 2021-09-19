from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
# from operators.stage_redshift import StageToRedshiftOperator
# from operators.load_fact import LoadFactOperator
# from operators.load_dimension import LoadDimensionOperator
# from operators.data_quality import DataQualityOperator

from helpers import SqlQueries
import configparser

config = configparser.ConfigParser()
config.read_file(open('dags/dwh.cfg'))

AWS_KEY = config.get('AWS','KEY')
AWS_SECRET = config.get('AWS','SECRET')

default_args = {
    'owner': 'ljia-ch',
    'denpends_on_past': False,
    'email':['ljia24338@gmail.com']
    'email_on_failure'：False,
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('sparkify_airflow_proj_dag',
          default_args=default_args,
          description='Load data from S3 and transform in Redshift with Airflow',
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
    dag=dag,
    postgres_conn_id='redshift',
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
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
