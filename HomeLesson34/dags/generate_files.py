from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import openpyxl
from utils.faker_user import generate_random_records, generate_random_sells


def start_time_generation_files() -> str:
    """Функция получения времени генерации файлов"""
    start_generation = datetime.now().strftime("%Y.%m.%d_%H.%M.%S")
    print(f"Время создания файла: {start_generation}")
    return start_generation


date_start_geberation = start_time_generation_files()



def generate_xlsx_users(path="/opt/airflow/dags/data/", filename="users.xlsx"):
    """Функция для сохранения сгенерированных данных о пользователе в xlsx"""

    # Создаем новую книгу Excel
    workbook = openpyxl.Workbook()
    sheet = workbook.active

    data = generate_random_records()

    # Записываем заголовки (если нужно)
    sheet.append(["Имя", "Фамилия", "Город"])

    # Обработка каждой строки
    for record in data:
        # Разделяем строку по запятой и преобразуем в список
        split_record = record.split(",")
        # Добавляем запись в таблицу
        sheet.append(split_record)

    # Сохраняем файл
    workbook.save(f"{path}{date_start_geberation}_{filename}")
    count_rows = workbook.active.max_row
    full_filename_xlsx = f"{date_start_geberation}_{filename}"

    print(f'Файл "{full_filename_xlsx}" успешно создан!')

    return count_rows, full_filename_xlsx


def generate_txt_sells(path="/opt/airflow/dags/data/", filename_txt="sells.txt"):
    full_filename_txt = f"{date_start_geberation}_{filename_txt}"
    with open(f"{path}{full_filename_txt}", "w", encoding="utf-8") as file:  
        data_sells = generate_random_sells()
        count_rows_txt = len(data_sells)
        for line in data_sells:
            file.write(f"{line[0]}, {line[1]}, {line[2]}\n")

    print(f'Файл "{full_filename_txt}" успешно создан!')  

    return count_rows_txt, full_filename_txt


def open_files_and_count_rows():
    """Функция для открытия сгенерированных файлов и подсчета количества строк"""

    count_rows_xlsx, full_filename_xlsx = generate_xlsx_users()
    count_rows_txt, full_filename_txt = generate_txt_sells()
    print(f"Количество строк в файле: {full_filename_xlsx}: {count_rows_xlsx}")
    print(f"Количество строк в файле: {full_filename_txt}: {count_rows_txt}")


default_args = {
    "owner": "serzik",
}

with DAG(
    "Generation_files",
    description="Созадние файла xlsx с генерированными пользователями txt файла с генерированными продажами",
    schedule_interval=None,
    start_date=datetime(2025, 6, 13),
) as dag:

    time_generate_files_task = PythonOperator(
        task_id="time_generate_files", python_callable=start_time_generation_files
    )

    generate_xlsx_task = PythonOperator(
        task_id="generate_xlsx_file", python_callable=generate_xlsx_users
    )

    generate_txt_task = PythonOperator(
        task_id="generate_txt_file", python_callable=generate_txt_sells
    )

    open_files_and_count_rows_task = PythonOperator(
        task_id="open_files_and_count_rows", python_callable=open_files_and_count_rows
    )

    (
        time_generate_files_task 
        >> [generate_xlsx_task, generate_txt_task]
        >> open_files_and_count_rows_task
    )
