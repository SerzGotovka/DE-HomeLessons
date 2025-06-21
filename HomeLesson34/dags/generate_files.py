from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import openpyxl
from utils.faker_user import generate_random_records, generate_random_sells

def start_time_generation_files() -> str:
    """Функция получения времени генерации файлов"""
    start_generation = datetime.now().strftime("%Y.%m.%d_%H.%M.%S")
    
    return start_generation

def generate_xlsx_users(path="/opt/airflow/dags/data/", filename="users.xlsx"):
    """Функция для сохранения сгенерированных данных о пользователе в xlsx"""
    workbook = openpyxl.Workbook()
    sheet = workbook.active

    data = generate_random_records()
    sheet.append(["Имя", "Фамилия", "Город"])

    for record in data:
        split_record = record.split(",")
        sheet.append(split_record)

    # Сохраняем файл с дата-именем
    full_filename_xlsx = f"{path}{filename}"
    workbook.save(full_filename_xlsx)
    count_rows = workbook.active.max_row

    return count_rows, full_filename_xlsx

def generate_txt_sells(path="/opt/airflow/dags/data/", filename_txt="sells.txt"):
    """Функция для сохранения сгенерированных данных о продажах в текстовый файл"""
    full_filename_txt = f"{path}{filename_txt}"
    
    with open(full_filename_txt, "w", encoding="utf-8") as file:
        data_sells = generate_random_sells()
        count_rows_txt = len(data_sells)
        for line in data_sells:
            file.write(f"{line[0]}, {line[1]}, {line[2]}\n")

    return count_rows_txt, full_filename_txt

def open_files_and_count_rows():
    """Функция для открытия сгенерированных файлов и подсчета количества строк"""
    count_rows_xlsx, full_filename_xlsx = generate_xlsx_users()
    count_rows_txt, full_filename_txt = generate_txt_sells()
    time_generation_files = start_time_generation_files()
    print(f'Время начала генерации файлов: {time_generation_files}')
    print(f"Количество строк в файле: {full_filename_xlsx}: {count_rows_xlsx}")
    print(f"Количество строк в файле: {full_filename_txt}: {count_rows_txt}")

default_args = {
    "owner": "serzik",
}

with DAG(
    "34_Generation_files",
    description="Создание файла xlsx с генерированными пользователями и txt файла с генерированными продажами",
    schedule_interval=None,
    start_date=datetime(2025, 6, 13),
) as dag:

    # Генерация времени создания файлов
    time_generate_files_task = PythonOperator(
        task_id="time_generate_files", 
        python_callable=start_time_generation_files
    )

    # Генерация файла Excel
    generate_xlsx_task = PythonOperator(
        task_id="generate_xlsx_file",
        python_callable=generate_xlsx_users
    )

    # Генерация текстового файла
    generate_txt_task = PythonOperator(
        task_id="generate_txt_file",
        python_callable=generate_txt_sells
    )

    # Подсчет строк в файлах
    open_files_and_count_rows_task = PythonOperator(
        task_id="open_files_and_count_rows", 
        python_callable=open_files_and_count_rows
    )

    # Определение порядка выполнения задач
    time_generate_files_task >> [generate_xlsx_task, generate_txt_task] >> open_files_and_count_rows_task