from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/opt/airflow/src')

from job2_cleaner import run_cleaner

default_args = {
    'owner': 'team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'job2_weather_cleaning',
    default_args=default_args,
    description='Hourly batch job: read Kafka, clean data, write to SQLite',
    schedule_interval='@hourly', 
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=['weather', 'cleaning', 'batch'],
)

def cleaning_task():
 
    run_cleaner()

clean_and_store = PythonOperator(
    task_id='clean_and_store_weather',
    python_callable=cleaning_task,
    dag=dag,
)

clean_and_store