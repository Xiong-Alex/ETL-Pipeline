from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="weather_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    clean_weather = SparkSubmitOperator(
        task_id="clean_weather",
        application="/opt/airflow/spark/clean_weather_minimal.py",
        conn_id="spark_default",
        application_args=[
            "--raw", "/opt/data/raw/weather_mock1.csv",
            "--curated", "/opt/data/curated/weather_daily",
            "--rejected", "/opt/data/rejected/weather_daily",
        ],
    )
