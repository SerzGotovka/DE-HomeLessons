from faker import Faker
import random

def generate_random_records(num_records=10):
    """ Функция для случайной генерации пользователей """
    num_records = max(10, min(num_records, 20))
    
    fake = Faker()  # Инициализация Faker
    records = []

    for _ in range(num_records):
        name = fake.first_name()         # Генерация имени
        surname = fake.last_name()       # Генерация фамилии
        city = fake.city()               # Генерация города
        
        record = f"{name},{surname},{city}"  
        records.append(record)

    return records

# # Пример использования функции
# if __name__ == "__main__":
#     generated_records = generate_random_records(random.randint(10, 20))
#     for record in generated_records:
#         print(record)



def generate_random_sells(num_records=30):
    """ Функция для случайной генерации продаж """
    num_records = max(30, min(num_records, 80))
    
    fake = Faker()  # Инициализация Faker
    
    products = ["Бананы", "Яблоки", "Апельсины", "Груши", "Киви", 
                "Виноград", "Персики", "Сливы", "Клубника", "Малина"]
    
    sales_records = []

    for _ in range(num_records):
        product = random.choice(products)  # Случайный выбор товара
        quantity = random.randint(1, 10)    # Случайное количество (от 1 до 10)
        price = random.randint(50, 200)      # Случайная цена (от 50 до 200 р)
        
        record = [product, f"{quantity}шт", f"{price}р"]  # Кортеж формата (товар, количество, цена)
        sales_records.append(record)

    return sales_records

if __name__ == "__main__":
    sales_data = generate_random_sells()
    print(sales_data)