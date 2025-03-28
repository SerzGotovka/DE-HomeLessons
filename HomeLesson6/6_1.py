"""Задача 1. Вычисление корней квадратного уравнения
Напишите программу, которая:
- Запрашивает у пользователя коэффициенты квадратного уравнения (a, b, c) (тип float).
- Вычисляет дискриминант по формуле: см. скриншот
- Вычисляет корни уравнения по формулам (будем считать, что дискриминант всегда больше 0): : см. скриншот
- Выводит результат.
(подсказка – корень можно реализовать с помощью возведения в степень)"""

a = float(input("Введите коэффициент a: "))
b = float(input("Введите коэффициент b: "))
c = float(input("Введите коэффициент c: "))

d = b**2-4*a*c

if d > 0:
    x1 = (-b + d**0.5)/(2*a)
    x2 = (-b - d**0.5)/(2*a)
    print(f"Корни уравнения: {x1} и {x2}")
else:
    print("Дискриминант меньше 0, корней нет")

