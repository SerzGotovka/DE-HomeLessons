from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils_41.function import generate_data


with DAG(
    "41_GENERATE_DATA",
    description="Генерация данных",
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2025, 7, 9),
    catchup=True,
    tags=["generate"],
) as dag:

    generate_task = PythonOperator(
        task_id="generate_data_task", python_callable=generate_data
    )
