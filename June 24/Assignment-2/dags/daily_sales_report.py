from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowFailException
import pandas as pd
import os
import shutil

default_args = {
    'owner': 'airflow',
    'execution_timeout': timedelta(minutes=5),  # Fail if a task runs longer than 5 min
}

# File paths
DATA_PATH = '/opt/airflow/data/sales.csv'
ARCHIVE_PATH = '/opt/airflow/archive/sales.csv'
SUMMARY_PATH = '/opt/airflow/output/sales_summary.csv'

def archive_original():
    if not os.path.exists(DATA_PATH):
        raise AirflowFailException(f"Source file not found: {DATA_PATH}")
    os.makedirs(os.path.dirname(ARCHIVE_PATH), exist_ok=True)
    shutil.move(DATA_PATH, ARCHIVE_PATH)
    print(f"Archived original file to: {ARCHIVE_PATH}")

def read_and_summarize():
    if not os.path.exists(ARCHIVE_PATH):
        raise FileNotFoundError(f"{ARCHIVE_PATH} not found.")
    
    df = pd.read_csv(ARCHIVE_PATH)
    summary = df.groupby('category')['amount'].sum().reset_index()
    os.makedirs(os.path.dirname(SUMMARY_PATH), exist_ok=True)
    summary.to_csv(SUMMARY_PATH, index=False)
    print(f"Summary saved at: {SUMMARY_PATH}")


with DAG(
    dag_id='daily_sales_report',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='0 6 * * *',  # Run daily at 6 AM
    catchup=False,
    tags=['assignment2'],
) as dag:

    archive = PythonOperator(
        task_id='archive_sales_csv',
        python_callable=archive_original
    )

    summarize = PythonOperator(
        task_id='summarize_sales',
        python_callable=read_and_summarize
    )

    archive >> summarize
