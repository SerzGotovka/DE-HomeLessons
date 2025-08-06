from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import os
from utils_41.function import (
    extract_raw_data,
    transform_and_load_users,
    transform_and_load_products,
    transform_and_load_orders,
)


@dag(
    dag_id="41_load_normalization_table_to_dds",
    description="Нормализация таблицы raw.orders и загрузка таблиц в dds слой с динамическим созданием таблиц в dds",
    schedule_interval="* * * * *",
    start_date=datetime(2025, 7, 10),
    catchup=False,
    max_active_runs=1,
    tags=["dds", "normalization"],
)
def build_dds_layer():
    # Создание таблиц в dds слое
    SQL_DIR = os.path.join(os.path.dirname(__file__), "sql", "dds_products")
    table_creation = ["users", "products", "orders"]

    prev_task = None
    for table_name in table_creation:
        # Найти путь к файлику
        sql_file_path = os.path.join(SQL_DIR, f"create_{table_name}.sql")
        relative_sql_path = os.path.relpath(sql_file_path, os.path.dirname(__file__))

        # Создание оператора создания таблички
        create_table_task = PostgresOperator(
            task_id=f"create_table_{table_name}",
            postgres_conn_id="my_postgres_conn",
            sql=relative_sql_path,
        )

        # Логика запуска
        if prev_task:
            prev_task >> create_table_task

        prev_task = create_table_task

    df = extract_raw_data()
    transform_and_load_users(df)
    transform_and_load_products(df)
    transform_and_load_orders(df)


build_dds_layer()
