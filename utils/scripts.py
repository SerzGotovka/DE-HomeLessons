import sqlite3
import pandas as pd
from typing import Dict
import psycopg2

def read_file(filename: str) -> list:
    """Функция для чтения файла"""
    try:
        with open(filename, "r", encoding="utf-8") as file:
            content = file.readlines()
            return content  # Возвращаем содержимое файла, а не преобразованные числа
    except FileNotFoundError:
        print("Файл не найден!")
        return []
    except Exception as e:
        print(f"Ошибка при работе с файлом: {e}")
        return []
    

def write_files(filename: str, data: list) -> None:
    """Функция для записи файлов"""
    try:
        with open(filename, "w", encoding="utf-8") as file:
            for item in data:
                file.write(f"{str(item)}, ")
    except Exception as e:
        print(f"Ошибка с созданием файла: {e}")    


def load_data_from_db(db_path: str) -> Dict:
    """Загружает данные из базы данных SQLite и сохраняет их в словаре DataFrame."""
    dataframes = {}
    try:
        with sqlite3.connect(db_path) as conn:
            tables_query = "SELECT name FROM sqlite_master WHERE type='table';"
            name_tables = pd.read_sql(tables_query, conn)
            names_list = name_tables["name"].tolist()

            for name in names_list:
                df = pd.read_sql(f"SELECT * FROM {name}", conn)
                dataframes[name] = df
                df.to_excel(f"data/{name}.xlsx", index=False)  # Сохраняем в Excel
    except Exception as e:
        print(f"Ошибка при загрузке данных из базы: {e}")
    return dataframes


def get_connection():
    return psycopg2.connect(
        host='localhost',
        database='demo',
        user='postgres',
        password='postgres',
        port=5432
    )