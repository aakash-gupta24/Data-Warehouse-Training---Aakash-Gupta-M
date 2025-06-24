from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os

FILE_PATH = '/opt/airflow/data/log.txt'
SIZE_THRESHOLD_MB = 1
SIZE_THRESHOLD_BYTES = SIZE_THRESHOLD_MB * 1024 * 1024

def check_file_size():
    if not os.path.exists(FILE_PATH):
        raise FileNotFoundError(f"{FILE_PATH} does not exist.")
    size = os.path.getsize(FILE_PATH)
    print(f"File size: {size} bytes")
    if size > SIZE_THRESHOLD_BYTES:
        return 'cleanup_log'
    else:
        return 'skip_cleanup'

def dummy_cleanup():
    print("Cleaning up the large file...")

with DAG(
    dag_id='file_size_branching',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['assignment4'],
) as dag:

    branch = BranchPythonOperator(
        task_id='check_file_size',
        python_callable=check_file_size,
    )

    cleanup = PythonOperator(
        task_id='cleanup_log',
        python_callable=dummy_cleanup,
    )

    skip = BashOperator(
        task_id='skip_cleanup',
        bash_command='echo "File is small, no cleanup needed."'
    )

    branch >> [cleanup, skip]
