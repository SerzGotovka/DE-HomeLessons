from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import logging
import os
import glob
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable


logger = logging.getLogger()
logger.setLevel("INFO")

DATA_DIR = "/opt/airflow/dags/input/"
PROCESSED_DIR = "/opt/airflow/dags/processed/"

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)


class FileSensorWithXCom(FileSensor):
    def poke(self, context):
        files = glob.glob(self.filepath)
        if files:
            context["ti"].xcom_push(key="file_path", value=files[0])
            return True
        return False


def load_data_deliveries_to_postgres(**kwargs):
    table_name = kwargs["table_name"]
    ti = kwargs["ti"]

    file_path = ti.xcom_pull(task_ids=f"wait_for_{table_name}", key="file_path")

    try:
        df = pd.read_csv(file_path)

        pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
        engine = pg_hook.get_sqlalchemy_engine()

        df.to_sql(table_name, engine, schema="raw", if_exists="append", index=False)

        logger.info(f"Файл {file_path} загружен")

        processed_path = os.path.join(PROCESSED_DIR, os.path.basename(file_path))
        os.rename(file_path, processed_path)
        logger.info(f"Файл перемещён в {processed_path}")

        return {
            "table_name": table_name,
            "file_name": os.path.basename(file_path),
            "rows_inserted": len(df),
        }

    except:
        logger.error(f"Ошибка при загрузке {file_path}")
        raise


with DAG(
    "37_load_data_deliveries_to_postgres",
    description="Вставка данных в постгрес из файлов",
    schedule_interval="* * * * *",
    start_date=datetime(2025, 6, 26),
    catchup=False,
    max_active_runs=1,
) as dag:

    wait_for_deliveries = FileSensorWithXCom(
        task_id="wait_for_deliveries",
        fs_conn_id="fs_default",
        filepath=f"{DATA_DIR}deliveries_*.csv",
        poke_interval=10,
        timeout=30 * 5,
    )

    create_deliveries_table = PostgresOperator(
        task_id="create_deliveries_table",
        postgres_conn_id="my_postgres_conn",
        sql="""
    CREATE TABLE IF NOT EXISTS raw.deliveries (
        delivery_id TEXT,
        order_id TEXT,   
        product TEXT,
        company TEXT,      
        cost FLOAT,   
        courier_name TEXT,       
        courier_phone TEXT,    
        start_time DATE,
        end_time DATE,
        city TEXT,
        warehouse TEXT,     
        address TEXT
    );
""",
    )

    load_deliveries_task = PythonOperator(
        task_id="load_deliveries_data",
        python_callable=load_data_deliveries_to_postgres,
        op_kwargs={"table_name": "deliveries"},
        provide_context=True,
    )

    print_date_deliveries = SimpleHttpOperator(
        task_id="send_tg_message",
        http_conn_id="telegram_bot",
        endpoint=f"{Variable.get('api_bot')}/sendMessage?chat_id={Variable.get('id_chat_tg_bot')}&text="
        "Таблица {{ ti.xcom_pull(task_ids='load_deliveries_data')['table_name'] }}: загружено {{ ti.xcom_pull(task_ids='load_deliveries_data')['rows_inserted'] }} строк (файл {{ ti.xcom_pull(task_ids='load_deliveries_data')['file_name'] }}). ",
        method="POST",
        headers={"Content-Type": "application/json"},
        log_response=True,
    )

(
    wait_for_deliveries
    >> create_deliveries_table
    >> load_deliveries_task
    >> print_date_deliveries
)
