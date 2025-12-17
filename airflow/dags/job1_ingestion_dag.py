from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/opt/airflow/src')

from job1_producer import run_producer

default_args = {
    'owner': 'team',
    'depends_on_past': False,
    'retries': 1,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'job1_weather_ingestion',
    default_args=default_args,
    description='Continuous data ingestion (Pseudo Streaming)',
    schedule_interval='@hourly', 
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=['weather', 'ingestion'],
)

def ingestion_task():
    run_producer(duration_minutes=50)

ingest_weather_data = PythonOperator(
    task_id='ingest_weather_data',
    python_callable=ingestion_task,
    dag=dag,
)