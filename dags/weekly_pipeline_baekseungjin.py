from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import os
from pathlib import Path
import pendulum

S3_KEY = "bronze/netflix_titles.csv"

INPUT_PATH = "/opt/airflow/data/netflix_titles.csv"
OUTPUT_PATH = "/opt/airflow/output/netflix_aggregate"

def download_csv():
    bucket_name = os.environ["S3_BUCKET_NAME"]

    Path("/opt/airflow/data").mkdir(
        parents=True,
        exist_ok=True,
    )

    s3 = S3Hook(
        aws_conn_id="aws_default"
    ).get_conn()

    s3.download_file(
        bucket_name,
        S3_KEY,
        INPUT_PATH,
    )

    print(
        f"Downloaded: s3://{bucket_name}/{S3_KEY}"
    )
    print(
        f"Saved to: {INPUT_PATH}"
    )

def upload_silver(**context):
    bucket_name = os.environ["S3_BUCKET_NAME"]

    logical_date = context["logical_date"].in_timezone(
        "Asia/Seoul"
    )

    date_str = logical_date.strftime("%Y-%m-%d")

    s3_prefix = f"silver/{date_str}/"

    s3 = S3Hook(
        aws_conn_id="aws_default"
    ).get_conn()

    uploaded_keys = []

    for file_path in Path(OUTPUT_PATH).rglob("*.parquet"):
        relative_path = file_path.relative_to(
            OUTPUT_PATH
        )

        s3_key = f"{s3_prefix}{relative_path}"

        s3.upload_file(
            str(file_path),
            bucket_name,
            s3_key,
        )

        uploaded_keys.append(s3_key)

    print(
        f"Uploaded parquet file count: {len(uploaded_keys)}"
    )

    for key in uploaded_keys:
        print(
            f"Uploaded: s3://{bucket_name}/{key}"
        )

    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=s3_prefix,
    )

    objects = response.get("Contents", [])

    print(
        f"S3 silver object count: {len(objects)}"
    )

    for obj in objects:
        print(
            f"{obj['Key']} ({obj['Size']} bytes)"
        )

with DAG(
    dag_id="weekly_pipeline_baekseungjin",
    start_date=pendulum.datetime(
        2026,
        9,
        17,
        tz="Asia/Seoul",
    ),
    schedule=None,
    catchup=False,
    params={
        "release_year": 2015,
    },
) as dag:

    download_csv_task = PythonOperator(
        task_id="download_csv",
        python_callable=download_csv,
    )

    transform_task = BashOperator(
        task_id="transform",
        bash_command="""
        spark-submit /opt/airflow/dags/jobs/transform.py \
          --input /opt/airflow/data/netflix_titles.csv \
          --output /opt/airflow/output/netflix_aggregate \
          --release-year {{ params.release_year }}
        """,
    )

    upload_silver_task = PythonOperator(
        task_id="upload_silver",
        python_callable=upload_silver,
    )

    download_csv_task >> transform_task >> upload_silver_task