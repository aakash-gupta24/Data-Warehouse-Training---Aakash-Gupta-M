from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os
import pandas as pd

default_args = {
    'owner': 'airflow',
}

DATA_PATH = '/opt/airflow/data/customers.csv'  
ROW_COUNT_THRESHOLD = 100

def check_file_exists():
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"{DATA_PATH} not found.")

def count_rows_and_log(**kwargs):
    df = pd.read_csv(DATA_PATH)
    row_count = len(df)
    print(f"Row count: {row_count}")
    kwargs['ti'].xcom_push(key='row_count', value=row_count)

def decide_to_alert(**kwargs):
    row_count = kwargs['ti'].xcom_pull(key='row_count', task_ids='count_rows')
    if row_count > ROW_COUNT_THRESHOLD:
        return 'send_alert'
    return 'end'

with DAG(
    dag_id='csv_summary_dag',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['assignment1'],
) as dag:

    t1 = PythonOperator(
        task_id='check_file',
        python_callable=check_file_exists
    )

    t2 = PythonOperator(
        task_id='count_rows',
        python_callable=count_rows_and_log,
        provide_context=True
    )

    t3 = BranchPythonOperator(
        task_id='check_if_alert_needed',
        python_callable=decide_to_alert,
        provide_context=True
    )

    t4 = BashOperator(
        task_id='send_alert',
        bash_command='echo "Row count exceeded 100!"'
    )

    t5 = BashOperator(
        task_id='end',
        bash_command='echo "No alert needed."'
    )


    t1 >> t2 >> t3 >> [t4, t5]
