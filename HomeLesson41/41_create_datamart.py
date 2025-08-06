from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import os
from utils_41.function import upload_data_to_datamart


@dag(
    dag_id="41_load_data_to_datamart",
    description="Создание витрин и загрузка данных в data_mart",
    schedule_interval="* * * * *",
    start_date=datetime(2025, 7, 17),
    catchup=False,
    max_active_runs=1,
    tags=["data_mart"],
)
def build_datamart_layer():

    SQL_DIR = os.path.join(os.path.dirname(__file__), "sql", "datamart_product")
    table_creation = ["dm_order_trends"]

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

    upload_data_to_datamart()


build_datamart_layer()
