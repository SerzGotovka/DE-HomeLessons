from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from function.home_34_func import (
    read_xlsx_and_load_to_posrgtres,
    read_orders_and_load_to_posrgtres,
    read_deliveries_and_load_to_posrgtres,
)
from function.home_35_func import read_raw_users_orders_transform_and_load_data_mart


default_args = {
    "owner": "Serzik",
}

with DAG(
    "35_import_data_from_excel",
    default_args=default_args,
    description="Интеграция данных из xlsx",
    schedule_interval=None,
    start_date=datetime(2025, 6, 1),
) as dag:
    d = DummyOperator(task_id="dummy")

    read_users_and_load_task = PythonOperator(
        task_id="read_users_and_load_raw_users",
        python_callable=read_xlsx_and_load_to_posrgtres,
    )

    read_orders_and_load_task = PythonOperator(
        task_id="read_orders_and_load_raw_orders",
        python_callable=read_orders_and_load_to_posrgtres,
    )

    read_delivers_and_load_task = PythonOperator(
        task_id="read_delivers_and_load_raw_delivers",
        python_callable=read_deliveries_and_load_to_posrgtres,
    )

    read_raw_users_orders_transform_and_load_data_mart_task = PythonOperator(
        task_id="read_raw_users_orders_transform_and_load_data_mart",
        python_callable=read_raw_users_orders_transform_and_load_data_mart,
    )

    (
        d
        >> [
            read_users_and_load_task,
            read_orders_and_load_task,
            read_delivers_and_load_task,
        ]
        >> read_raw_users_orders_transform_and_load_data_mart_task
    )
