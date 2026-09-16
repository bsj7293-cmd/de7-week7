from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta
import pendulum

def count_lines(ti):
    print(f"try_number = {ti.try_number}")

    # 첫 번째 시도만 의도적으로 실패
    if ti.try_number == 1:
        raise ValueError("Intentional failure on first attempt")

    value = len(["airflow", "xcom", "retry", "week7"])
    print(f"Calculated value: {value}")

    return value

def use_value(ti):
    value = ti.xcom_pull(
        task_ids="count_lines",
        key="return_value"
    )

    print(f"Pulled XCom value: {value}")

with DAG(
    dag_id="xcom_demo_baekseungjin",
    start_date=pendulum.datetime(2026, 9, 17, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
) as dag:

    count_lines_task = PythonOperator(
        task_id="count_lines",
        python_callable=count_lines,
        retries=2,
        retry_delay=timedelta(seconds=10),
    )

    use_value_task = PythonOperator(
        task_id="use_value",
        python_callable=use_value,
    )

    count_lines_task >> use_value_task