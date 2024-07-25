from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
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


def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        query = file.read()
    return query

sql_query_path = '/opt/airflow/scripts/Data_Processing/athena_Processing.sql'  # Replace with the actual path to your SQL file
athena_query = read_sql_file(sql_query_path)


fetch_data_task = PythonOperator(
    task_id='fetch_and_process_data',
    python_callable=fetch_and_process_data,
    dag=dag
)


athena_query_task = AWSAthenaOperator(
    task_id='run_athena_query',
    query=athena_query, 
    database='stockdb', 
    output_location='s3://athena-result-bucket-ankit86/codeoutput/',
    aws_conn_id='aws_default',
    dag=dag
)

fetch_data_task >> athena_query_task


# arn:aws:sns:ap-south-1:590183947745:stockBreakoutSNS