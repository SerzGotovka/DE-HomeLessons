"""
1. Нужно создать файлы:
Каждый файл имеет формат sales_YYYY_MM.txt, где YYYY — год, а MM — месяц. Внутри каждого файла данные представлены в формате:
Дата:Сумма продаж:Менеджер.

Пример файла sales_2023_01.txt:

2023-01-01:1000:Иван Иванов
2023-01-02:1500:Петр Петров
2023-01-03:2000:Мария Сидорова

2. Напишите скрипт, который:
- Считает общую сумму продаж за все месяцы.
- Находит менеджера с наибольшей суммой продаж.
- Сохраняет результаты в файл report.txt в формате:
Общая сумма продаж: <сумма>
Лучший менеджер: <имя менеджера>
"""

data_2023_01 = [
    "2023-01-01:1000:Иван Иванов",
    "2023-01-02:1500:Петр Петров",
    "2023-01-03:2000:Мария Сидорова"
]

data_2023_02 = [
    "2023-02-01:1200:Иван Иванов",
    "2023-02-02:1800:Петр Петров",
    "2023-02-03:2200:Мария Сидорова"
]

data_2023_03 = [
    "2023-03-01:1300:Иван Иванов",
    "2023-03-02:1700:Петр Петров",
    "2023-03-03:2100:Мария Сидорова"
]


def create_sales_files():
    """Функция для создания файлов с данными о продажах"""
    
    sales_data = {
        "sales_2023_01.txt": data_2023_01,
        "sales_2023_02.txt": data_2023_02,
        "sales_2023_03.txt": data_2023_03
    }
    
    for filename, data in sales_data.items():
        with open(f'HomeLesson10/{filename}', 'w', encoding='utf-8') as file:
            file.writelines(list(map(lambda x: x + '\n', data)))
            


def analyze_sales():
    """Функция для анализа данных о продажах"""
    total_sales = 0
    manager_sales = {}  

    
    files = ["sales_2023_01.txt", "sales_2023_02.txt", "sales_2023_03.txt"]
    
    for filename in files:
        with open(filename, 'r', encoding='utf-8') as file:
            for line in file:
                date, amount, manager = line.strip().split(':') 
                amount = int(amount)  
                
                total_sales += amount  
                
                
                if manager in manager_sales:
                    manager_sales[manager] += amount
                else:
                    manager_sales[manager] = amount

 
    best_manager = max(manager_sales, key=manager_sales.get)
    
    return total_sales, best_manager  


def write_report(total_sales, best_manager):
    """Функция для записи отчета в файл"""

    with open('HomeLesson10/report.txt', 'w', encoding='utf-8') as file:
        file.write(f"Общая сумма продаж: {total_sales}\n")  
        file.write(f"Лучший менеджер: {best_manager}\n")  


create_sales_files() 
total_sales, best_manager = analyze_sales() 
write_report(total_sales, best_manager)  

