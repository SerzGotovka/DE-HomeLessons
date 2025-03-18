"""
Задача 1:
Есть входной файлик по информации по людям, необходимо подготовить отчет в формате .txt в любом виде
с информацией о том, сколько по каждой профессии у нас людей, сколько людей в каждом городе и сколько людей в стране.

Вывод в файлике statistics.txt:
Статистика по профессиям:
программист: 10 человек
учительница: 17 человек
...

Статистика по городам:
Минск: 3 человек
Москва: 1 человек
...

Статистика по странам:
Беларусь: 23 человек
Россия: 26 человек
"""

with open('HomeLesson9/filename.txt', 'r', encoding='utf-8') as file:
  content = file.readlines()
  # print(content)

  posts_dict = dict()
  cities_dict = dict()
  countries_dict = dict()

  for i in content:
    data = i.strip().split(', ')

    post = data[5]
    cities = data[6]
    countries = data[7]

    if post in posts_dict:
      posts_dict[post] += 1
    else:
      posts_dict[post] = 1

    if cities in cities_dict:
      cities_dict[cities] += 1
    else:
      cities_dict[cities] = 1

    if countries in countries_dict:
      countries_dict[countries] += 1
    else:
      countries_dict[countries] = 1


with open('HomeLesson9/statistic.txt', 'w', encoding='utf-8') as file2:
  file2.write('Статистика по профессиям: \n')
  for prof, count_prof in posts_dict.items():
    file2.write(f'{prof}: {count_prof} человек \n')

  file2.write('\nСтатистика по городам: \n')
  for city_name, count_city in cities_dict.items():
    file2.write(f'{city_name}: {count_city} человек \n')

  file2.write('\nСтатистика по странам: \n')
  for country_name, count_country in countries_dict.items():
    file2.write(f'{country_name}: {count_country} человек \n')

  print(posts_dict, cities_dict, countries_dict)

