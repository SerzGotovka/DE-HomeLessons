"""
Задача 2.
Создайте программу, которая считывает список чисел из файла, проверяет каждое число на чётность
и записывает результаты (чётное или нечётное) в другой файл.
Используйте обработку исключений для возможных ошибок ввода-вывода.
"""

import sys
import os

# Добавляем путь к директории DE-HomeLessons
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.scripts import read_file, write_files


def even_odd_numbers(content: list) -> list:
    """Функция для создания списков четных и нечетных чисел"""
    try:
        data_numb = []
        for line in content:
            data = line.strip().split(",")
            data_numb.extend(
                map(int, data)
            )  # Преобразуем строки в числа и добавляем в список

        even_list = [x for x in data_numb if x % 2 == 0]
        odd_list = [x for x in data_numb if x % 2 != 0]

        return even_list, odd_list
    except Exception as e:
        print(f"Ошибка получения данных: {e}")
        return [], []


if __name__ == "__main__":
    content = read_file("HomeLesson11/example.txt")
    if content:  #! Проверяем, что данные были успешно прочитаны
        even_list, odd_list = even_odd_numbers(content)
        write_files("HomeLesson11/even_numb.txt", even_list)
        write_files("HomeLesson11/odd_numb.txt", odd_list)
