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