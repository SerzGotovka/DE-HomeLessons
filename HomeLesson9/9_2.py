"""Задача 2:
1. Объединение данных о студентах
Есть два файла: “students.txt” (ФИО, группа) и “grades.txt” (ФИО, оценки)
Задача: создать файл “report.txt” с информацией о студентах и их оценках, о среднем бале для каждого студента.
А так же в конце файла для каждой группы вывести средний бал по группе в порядке возрастания

Пример вывода:
Иванов Иван Иванович, Группа 101, 5 4 3 4 5, Средняя оценка: 4.20
Петров Петр Петрович, Группа 102, 4 4 5 3 4, Средняя оценка: 4.00
Сидоров Сидор Сидорович, Группа 101, 3 5 4 4 3, Средняя оценка: 3.80
Козлова Анна Сергеевна, Группа 102, 5 5 4 5 4, Средняя оценка: 4.60

Средние оценки по группам (в порядке возрастания):
Группа 101: 3.92
Группа 202: 4.30"""


students = {}  
with open('HomeLesson9/students.txt', 'r', encoding='utf-8') as f:
    for line in f:  
        name, group = line.strip().split(', ')  
        print(name, group)
        students[name] = group  


grades = {}  
with open('HomeLesson9/grades.txt', 'r', encoding='utf-8') as f:  
    for line in f:  
        parts = line.strip().split(', ') 
        name = parts[0] 
        marks = list(map(int, parts[1].split()))
        grades[name] = marks 


group_averages = {} 
with open('HomeLesson9/report.txt', 'w', encoding='utf-8') as f:  
    for name, group in students.items():
        if name in grades:  
            marks = grades[name] 
            average = sum(marks) / len(marks)  
            
            f.write(f"{name}, Группа {group}, {' '.join(map(str, marks))}, Средняя оценка: {average:.2f}\n")
            
            
            if group not in group_averages:
                group_averages[group] = []  
            group_averages[group].append(average)  

    
    f.write("\nСредние оценки по группам (в порядке возрастания):\n")
    for group in sorted(group_averages.keys()):  
        averages = group_averages[group]  
        group_average = sum(averages) / len(averages)  
        f.write(f"Группа {group}: {group_average:.2f}\n")
