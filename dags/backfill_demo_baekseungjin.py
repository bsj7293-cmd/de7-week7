from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pendulum
import os

OUTPUT_DIR = "/opt/airflow/output"

def write_daily_file(**context):
    logical_date = context["logical_date"].in_timezone("Asia/Seoul")
    date_str = logical_date.strftime("%Y-%m-%d")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    file_path = f"{OUTPUT_DIR}/daily_{date_str}.txt"

    with open(file_path, "w") as f:
        f.write(f"logical_date={logical_date}\n")
        f.write(f"date={date_str}\n")

    print(f"Created file: {file_path}")

with DAG(
    dag_id="backfill_demo_baekseungjin",
    start_date=pendulum.datetime(2026, 9, 10, tz="Asia/Seoul"),
    schedule="@daily",
    catchup=True,
) as dag:

    write_daily_file_task = PythonOperator(
        task_id="write_daily_file",
        python_callable=write_daily_file,
    )