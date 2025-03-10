"""
Задача 1: “Группировка по четности”
Условие:
Разделите список чисел на два кортежа: один с четными, другой с нечетными числами.

Вход: [1, 5, 76, 13, 12]
Выход: (76, 12), (1, 13)
"""

import sys

array_str = sys.argv[1]
array = array_str.split(',')
array = [int(x) for x in array]


tuple_even = tuple([x for x in array if x % 2 ==0 ])
tuple_odd = tuple([x for x in array if x % 2 != 0])

print(tuple_even)
print(tuple_odd)