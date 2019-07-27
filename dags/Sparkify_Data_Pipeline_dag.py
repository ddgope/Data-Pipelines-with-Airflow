from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
import sql_statements

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 26),    
    #'end_date': datetime(2018, 11, 12),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
}

dag = DAG('Sparkify_Data_Pipeline_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',          
          schedule_interval='0 0 * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_staging_events_table = PostgresOperator(
    task_id="create_staging_events_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_staging_events_TABLE_SQL
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events_from_s3_to_redshift",
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    #s3_key="log_data/2018/11/2018-11-01-events.json" 
    # .strftime("%d-%m-%Y")
    #s3_key="log_data/{execution_date.year}/{execution_date.month}/{execution_date.year}-{execution_date.month}-{execution_date.day}-events.json"   
    s3_key="log_data",
    JSONPaths="log_json_path.json"
)

create_staging_songs_table = PostgresOperator(
    task_id="create_staging_songs_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_staging_songs_TABLE_SQL
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs_from_s3_to_redshift",
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    JSONPaths="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift"   
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="users",
    sql_statement=sql_statements.user_table_insert,
    append_data=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift" ,
    table_name="songs",
    sql_statement=sql_statements.song_table_insert,
    append_data=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift" ,
    table_name="artists",
    sql_statement=sql_statements.artist_table_insert,
    append_data=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="time",
    sql_statement=sql_statements.time_table_insert,
    append_data=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    check_sql="SELECT COUNT(*) FROM  songplays",
    expected_value="320",
    describe="Fact table songplay - whether this table has data or not !"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_staging_events_table
start_operator >> create_staging_songs_table

create_staging_events_table >> stage_events_to_redshift
create_staging_songs_table >> stage_songs_to_redshift

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