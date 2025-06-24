from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from pathlib import Path
import pandas as pd

INPUT_DIR = Path("/opt/airflow/data/input")
OUTPUT_DIR = Path("/opt/airflow/data/output")
SUMMARY_FILE = OUTPUT_DIR / "summary.csv"
EXPECTED_HEADERS = ["id", "name", "quantity"]

default_args = {
    "owner": "airflow",
    "retries": 1
}

with DAG(
    dag_id="dynamic_csv_processing",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    description="Dynamically process CSV files in a folder",
) as dag:

    @task
    def list_csv_files():
        files = [str(file) for file in INPUT_DIR.glob("*.csv")]
        return files

    @task
    def process_file(file_path: str):
        df = pd.read_csv(file_path)
        
        if list(df.columns) != EXPECTED_HEADERS:
            return {"file": file_path, "status": "Invalid headers", "rows": 0}
        

        row_count = len(df)
        
        return {"file": Path(file_path).name, "status": "Valid", "rows": row_count}

    @task
    def merge_summaries(summaries: list):
        df = pd.DataFrame(summaries)
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        df.to_csv(SUMMARY_FILE, index=False)
        print(f"Summary written to {SUMMARY_FILE}")

    csv_files = list_csv_files()
    summaries = process_file.expand(file_path=csv_files)
    merge_summaries(summaries)