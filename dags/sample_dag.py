from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
import pendulum

def task_one():
    return "Hello Airflow"

def task_two():
    return "7week Airflow"

with DAG(
    dag_id="sample_dag",
    start_date=pendulum.datetime(2026, 9, 16, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=task_one,
    )

    goodbye_task = PythonOperator(
        task_id="goodbye_task",
        python_callable=task_two,
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> hello_task >> goodbye_task >> end