"""
Задача 2: Поиск минимального значения
У вас есть список чисел. Напишите программу, которая находит минимальное значение в списке через цикл for
"""

numbers = [10, 1, 5, 30, 15, 4, 65]
min_numbers = numbers[0]

for numb in numbers:
    if numb < min_numbers:
        min_numbers = numb
print(f'Минимальное значение: {min_numbers}.')
