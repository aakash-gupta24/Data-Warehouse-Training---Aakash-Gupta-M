from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
import random
import time

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(seconds=10),       # wait 10s before retry
    'retry_exponential_backoff': True,          # use exponential backoff
    'max_retry_delay': timedelta(minutes=1),    # max wait time
}

def flaky_api_call():
    value = random.choice([True, False])
    print(f"API call success? {value}")
    if not value:
        raise Exception("Simulated API failure")
    return "Success"

with DAG(
    dag_id='retry_with_alert',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['assignment3'],
) as dag:

    simulate_api = PythonOperator(
        task_id='simulate_api_call',
        python_callable=flaky_api_call
    )

    alert = BashOperator(
        task_id='alert_on_failure',
        bash_command='echo "ALERT: API call failed after retries!"',
        trigger_rule=TriggerRule.ONE_FAILED  # Run only if simulate_api fails
    )

    success = BashOperator(
        task_id='success_task',
        bash_command='echo "API call succeeded!"',
        trigger_rule=TriggerRule.ALL_SUCCESS  # Run only if simulate_api succeeds
    )

    simulate_api >> [alert, success]
