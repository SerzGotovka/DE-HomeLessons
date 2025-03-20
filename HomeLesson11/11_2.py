"""
Задача 2.
Создайте программу, которая считывает список чисел из файла, проверяет каждое число на чётность
и записывает результаты (чётное или нечётное) в другой файл.
Используйте обработку исключений для возможных ошибок ввода-вывода.
"""


def read_file(filename: str) -> list:
    """Функция для чтения файла"""
    with open(filename, "r", encoding="utf-8") as file:
        content = file.readlines()

        for line in content:
            data = line.strip().split(",")
            data_numb = list(map(lambda x: int(x), data))

            # print(data_numb)
        return data_numb


def even_odd_numbers(data: list) -> list:
    """Функиця для создания списков четных и нечетных чисел"""
    even_list = [x for x in data if x % 2 == 0]
    odd_list = [x for x in data if not x % 2 == 0]
    # print(even_list, odd_list)

    return even_list, odd_list


def write_files(filename: str, data: list) -> None:
    """Функция для записи файлов"""
    with open(filename, "w", encoding="utf-8") as file:
        for item in data:
            file.write(f"{str(item)}, ")


data = read_file("HomeLesson11\example.txt")
even_list, odd_list = even_odd_numbers(data)
write_files("HomeLesson11\even_numb.txt", even_list)
write_files("HomeLesson11\odd_numb.txt", odd_list)
