from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import os
from normalization.utils import (
    extract_raw_data,
    transform_and_load_cards,
    transform_and_load_couriers,
    transform_and_load_deliveries,
    transform_and_load_deliveries_company,
    transform_and_load_orders,
    transform_and_load_products,
    transform_and_load_users,
    transform_and_load_warehouse,
)


@dag(
    dag_id="38_build_dds_layer_potgres",
    start_date=datetime(2025, 7, 3),
    schedule_interval="0 19 * * *",
    tags=["dds"],
    description="Даг, который складывает данные из raw в dds слой в нормализованном виде",
    catchup=True,
)
def build_dds_layer():

    # Создание таблиц в dds слое
    SQL_DIR = os.path.join(os.path.dirname(__file__), "sql", "dds")
    table_creation_order = [
        "users",
        "products",
        "delivery_companies",
        "couriers",
        "warehouses",
        "orders",
        "deliveries",
        "cards",
    ]

    prev_task = None
    for table_name in table_creation_order:
        # Найти путь к файлику
        sql_file_path = os.path.join(SQL_DIR, f"create_{table_name}.sql")
        relative_sql_path = os.path.relpath(sql_file_path, os.path.dirname(__file__))

        # Созданиие оператора создания таблички
        create_table_task = PostgresOperator(
            task_id=f"create_table_{table_name}",
            postgres_conn_id="my_postgres_conn",
            sql=relative_sql_path,
        )

        # Логика запуска
        if prev_task:
            prev_task >> create_table_task

        prev_task = create_table_task

    df_users = extract_raw_data("users")
    transform_and_load_users(df_users)
    transform_and_load_cards(df_users)

    df_deliveries = extract_raw_data("deliveries")
    transform_and_load_products(df_deliveries)
    transform_and_load_deliveries_company(df_deliveries)
    transform_and_load_couriers(df_deliveries)
    transform_and_load_warehouse(df_deliveries)

    df_orders = extract_raw_data("orders")
    transform_and_load_orders(df_orders)

    df_deliveries = extract_raw_data("deliveries")
    transform_and_load_deliveries(df_deliveries)


build_dds_layer()
