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


def even_odd_numbers(content: list) -> tuple:
    """Функция для создания списков четных и нечетных чисел"""
    try:
        even_list = []
        odd_list = []
        
        for line in content:
            data = line.strip().split(",")
            for number in map(int, data):
                if number % 2 == 0:
                    even_list.append(number)  
                else:
                    odd_list.append(number)  

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
