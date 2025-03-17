"""
Условие:
У вас есть файл raw_data.txt, который содержит "сырые" данные о транзакциях. Данные имеют неструктурированный формат и могут содержать ошибки. Пример содержимого:
2023-01-01:1000:Иван Иванов
2023-01-02:1500:Петр Петров
2023-01-03:2000:Мария Сидорова
2023-01-04:ERROR
2023-01-05:2500:Ольга Кузнецова
2023-01-06:3000:ERROR

Задача:
Напишите скрипт, который:
Читает данные из файла raw_data.txt.
Удаляет строки с ошибками (где вместо значений стоит ERROR).
Преобразует оставшиеся данные в формат Дата,Сумма,Менеджер.
Сохраняет очищенные данные в новый файл cleaned_data.txt.
"""


def transform_data(filename):
    """Функция для чтения и преобразования файла"""
    try:
        with open(filename, "r", encoding="utf-8") as file:
            content = file.readlines()
            transform_data = list()

            for line in content:
                transform_data.append(line.strip().split(":"))

            return transform_data
    except FileNotFoundError:
        print("Файл не найден!")
    except:
        print("Ошибка при работе с файлом!")


def cleaned_data(data):
    """Функция для очистки данных"""
    try:
        cleaned_data = list()

        for i in data:
            if not "ERROR" in i:
                cleaned_data.append(i)
            else:
                continue

        return cleaned_data
    except:
        print("Ошибка получения очищенных данных")


def write_cleaned_data(filename, cleaned_data):
    """Функция для записи очищенных данных в файл"""
    try:
        with open(filename, "w", encoding="utf-8") as file:
            for item in cleaned_data:
                file.write(f"{item[0]}:{item[1]}:{item[2]}\n")
    except:
        print("Ошибка с созданием файла!")


norm_data = transform_data("HomeLesson10/raw_data.txt")
clean_data = cleaned_data(norm_data)
write_file = write_cleaned_data("HomeLesson10/cleaned_data.txt", clean_data)
