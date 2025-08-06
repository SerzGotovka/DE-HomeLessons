"""Домашнее задание 34 урока:
Задача. Реализовать даг в airflow, который генерирует 2 файла. Состоять она должна из 4 тасок:
1 таска. Пишет время начала генерации файлов
2-3 таска. Параллельно 2 таски генерируют: одна файл xlsx в которой сохраняет информацию сгенерированную по пользователям (можно рандомно через цикл сгенерировать 10-20 записей). Пример:
Василий,Смирнов,Минск
и второй файл в формате txt в который вы запищите сгенерированные данные рандомно сколько продаж было (тоже записей 30-80). Пример:
Бананы,3шт,100р
4 таска. Завершающая таска, которая открывает файлы сгенерированные и выводит кол-во строчек в каждом из файлов"""

from datetime import datetime
from faker_user import generate_random_records, generate_random_sells
import openpyxl

def start_time_generation_files() -> None:
    """ Функция получения времени генерации файлов"""
    start_generation = datetime.now().strftime('%Y.%m.%d_%H.%M.%S')
    print(f'Время создания файла: {start_generation}')

# start_time_generation_files()

def generate_xlsx_users(filename='HomeLesson34/data/users.xlsx'):
    """Функция для сохранения сгенерированных данных о пользователе в xlsx"""
   # Создаем новую книгу Excel
    workbook = openpyxl.Workbook()
    sheet = workbook.active

    data = generate_random_records()

    # Записываем заголовки (если нужно)
    sheet.append(['Имя', 'Фамилия', 'Город'])

    # Обработка каждой строки
    for record in data:
        # Разделяем строку по запятой и преобразуем в список
        split_record = record.split(',')
        # Добавляем запись в таблицу
        sheet.append(split_record)

    # Сохраняем файл
    workbook.save(filename)
    count_rows = workbook.active.max_row

    print(f'Файл "{filename}" успешно создан!')

    return count_rows

# generate_xlsx_users()


def generate_txt_sells(filename='HomeLesson34/data/sells.txt'):
    with open(filename, 'w', encoding='utf-8') as file:
        data_sells = generate_random_sells()
        count_rows_txt = len(data_sells)
        for line in data_sells:
            print(line)
            file.write(f'{line[0]}, {line[1]}, {line[2]}\n')
    print(f'Файл "{filename}" успешно создан!')
    
    return count_rows_txt

# generate_txt_sells()

def open_files_and_count_rouws():
    """Функция для открытия сгенерированных файлов и подсчета количества строк"""
    count_rows_xlsx = generate_xlsx_users()
    count_rows_txt = generate_txt_sells()
    print(f'Количество строк в файле xlsx: {count_rows_xlsx}')
    print(f'Количество строк в файле xlsx: {count_rows_txt}')
    with open('HomeLesson34/data/sells.txt', 'r', encoding='utf-8') as file:
        content = file.readlines()