from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from pathlib import Path
import pandas as pd
import csv

INPUT_DIR = Path("/opt/airflow/data/input")
OUTPUT_DIR = Path("/opt/airflow/data/output")
SUMMARY_FILE = OUTPUT_DIR / "summary.csv"
EXPECTED_HEADERS = ["id", "name", "quantity"]

default_args = {
    "owner": "airflow",
    "retries": 1
}

with DAG(
    dag_id="producer_consumer_dags",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    description="produce and read csv",
) as dag:

    @task
    def create_csv():
        INPUT_DIR.mkdir(parents=True, exist_ok=True)
        file_path = INPUT_DIR / "items.csv"
        data = [
            ["id", "name", "quantity"],
            [1, "Pen", 100],
            [2, "Pencil", 200],
            [3, "Notebook", 150]
        ]

        with open(file_path, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerows(data)
        return str(file_path)

    @task
    def read_csv(file_path: str):
        df = pd.read_csv(file_path)
        
        if list(df.columns) != EXPECTED_HEADERS:
            return {"file": file_path, "status": "Invalid headers", "rows": 0}
        

        row_count = len(df)
        print(f"No of rows=>>>>{row_count}")


    csv_path = create_csv()
    read_csv(csv_path)