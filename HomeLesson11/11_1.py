"""
Задача 1. 
Напишите функцию, которая принимает список чисел и возвращает их среднее значение.
Обработайте исключения, связанные с пустым списком и некорректными типами данных.
"""

def avg_values(list_numb: list) -> float:
    try:
        avg = sum(list_numb) / len(list_numb)
        print(avg)
        return avg
    
    except ZeroDivisionError:
        print("Введите значения!")
    except ValueError:
        print("Введите числовые значения")
    except TypeError:
        print("Введите числовые значения")


avg_values([5,7,10])
