""""
Задача 2: “Анаграммы”
Условие:
Проверьте, являются ли две строки анаграммами друг друга.

Вход: марш, шрам
Выход: Да, это “Анаграмма”
"""
import sys
user_str1 = sys.argv[1]
user_str2 = sys.argv[2]

# if sorted(user_str1) == sorted(user_str2):
#     print('СТроки аннограммы')

# else:
#     print('СТроки не являются аннограммами!')


if set(user_str2) == set(user_str1) and len(user_str1) == len(user_str2):
    print('СТроки аннограммы')

else:
    print('СТроки не являются аннограммами!')