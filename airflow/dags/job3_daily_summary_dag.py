from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/opt/airflow/src')

from job3_analytics import compute_daily_analytics
from db_utils import init_database

default_args = {
    'owner': 'team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'job3_daily_analytics',
    default_args=default_args,
    description='Daily analytics job: compute summary statistics and write to SQLite',
    schedule_interval='@daily',
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=['weather', 'analytics', 'daily'],
)

def init_db_task():
    init_database()  

init_db = PythonOperator(
    task_id='init_database',
    python_callable=init_db_task,
    dag=dag,
)

def analytics_task():

    compute_daily_analytics()

compute_analytics = PythonOperator(
    task_id='compute_daily_summary',
    python_callable=analytics_task,
    dag=dag,
)

init_db >> compute_analytics