from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

from utils_41.function import FileSensorWithXCom, load_data_orders_to_postgres, DATA_DIR

with DAG(
    "41_load_data_orders_to_postgres",
    description="Вставка данных в постгрес из файлов",
    schedule_interval="* * * * *",
    start_date=datetime(2025, 6, 26),
    catchup=False,
    max_active_runs=1,
    tags=["raw"],
) as dag:

    wait_for_orders = FileSensorWithXCom(
        task_id="wait_for_orders_41",
        fs_conn_id="fs_default",
        filepath=f"{DATA_DIR}orders_*.csv",
        poke_interval=10,
        timeout=30 * 5,
    )

    create_products_table = PostgresOperator(
        task_id="create_orders_table",
        postgres_conn_id="my_postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS raw.orders_41 (
                id_order INTEGER,   
                id_user TEXT, 
                fullname TEXT,
                email TEXT,
                phone TEXT,    
                product TEXT,
                date_orders DATE, 
                quantity int,
                price_per_unit float
            );
        """,
    )

    load_orders_task = PythonOperator(
        task_id="load_orders_data_to_raw",
        python_callable=load_data_orders_to_postgres,
        op_kwargs={"table_name": "orders_41"},
        provide_context=True,
    )


wait_for_orders >> create_products_table >> load_orders_task
