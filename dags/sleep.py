from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

def my_task():
    print("Starting my_task...")
    time.sleep(5)
    print("Finished my_task!")

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id='example_sleep_and_retry',
    description='A simple DAG that sleeps and retries if it fails',
    start_date=datetime(2024, 4, 1),
    schedule_interval='@hourly',  # run every hour
    catchup=False,
    default_args=default_args,
    tags=['example', 'test'],
) as dag:

    task1 = PythonOperator(
        task_id='sleep_task',
        python_callable=my_task,
    )

    task1
