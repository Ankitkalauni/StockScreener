from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_data_dag',
    default_args=default_args,
    description='Fetch and export data using mounted volumes',
    schedule_interval='30 12 * * 1-5',
    start_date=datetime(2024, 7, 19),
    catchup=False
)

def fetch_and_process_data():
    script_path = '/opt/airflow/scripts/Yahoo_api/nse_data_dumper.py'
    subprocess.run(['python', script_path], check=True)

fetch_data_task = PythonOperator(
    task_id='fetch_and_process_data',
    python_callable=fetch_and_process_data,
    dag=dag
)