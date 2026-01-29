from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello():
    print("✅ Airflow smoke test: task ran successfully")

with DAG(
    dag_id="smoke_airflow",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    PythonOperator(task_id="hello_task", python_callable=hello)
