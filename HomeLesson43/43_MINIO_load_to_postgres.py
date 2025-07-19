from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd


def get_and_load():
    hook = S3Hook(aws_conn_id="minio_default")                                      #Подключаемся к MinIO через S3Hook
    bucket_name = "data-bucket"                                                     #Указываем имя бакета (bucket) в MinIO, где лежат CSV-файлы.
    minio_client = hook.get_conn()                                                  #Получаем объект-клиент для работы с MinIO.
    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")                     #Подключаемся к PostgreSQL через PostgresHook

    # Подключаемся к PostgreSQL
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Получаем список уже обработанных файлов
    cursor.execute("SELECT name_file FROM raw.readed_files")                        #Получаем все записи из таблицы raw.readed_files
    processed_files = {row[0] for row in cursor.fetchall()}                         #Преобразуем результат в множество строк (set) для быстрого поиска.

    # Получаем список объектов из MinIO
    response = minio_client.list_objects_v2(Bucket=bucket_name, Prefix="raw/")      #Получаем список объектов из MinIO в папке raw/.
    contents = response.get("Contents", [])                                         #Извлекаем список файлов из ответа

    engine = pg_hook.get_sqlalchemy_engine()

    for obj in contents:                                                            #Проходим по каждому объекту (файлу) из MinIO.
        filekey = obj["Key"]                                                        #Получаем полный путь к файлу
        if filekey in processed_files:                                              #Проверяем, обрабатывали ли мы этот файл раньше.
            print(f"Файл уже обработан: {filekey}")
            continue                                                                #Если обрабатывали — пропускаем файл и идём к следующему.

        print(f"Обработка файла: {filekey}")
        obj_data = minio_client.get_object(Bucket=bucket_name, Key=filekey)         #Скачиваем содержимое файла из MinIO.
        df = pd.read_csv(obj_data["Body"])                                          #Читаем CSV-файл в pandas DataFrame из потока.

        df.to_sql("minio_data", engine, schema="raw", 
                  if_exists="append", index=False)
        print(f"Файл записан в PostgreSQL: {filekey}")

        # Добавляем файл в таблицу обработанных
        cursor.execute(
            "INSERT INTO raw.readed_files (name_file) VALUES (%s) ON CONFLICT DO NOTHING",
            (filekey,),
        )
        conn.commit()

    cursor.close()
    conn.close()


with DAG(
    "43_MINIO_load_to_postgres",
    start_date=datetime(2025, 7, 15),
    schedule_interval="@hourly",
    catchup=False,
) as dag:
    transfer_task = PythonOperator(
        task_id="transfer_task", python_callable=get_and_load
    )

    transfer_task
