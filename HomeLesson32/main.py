from clickhouse_driver import Client
import requests


# Подключение к серверу
client = Client(host='localhost')


# Create: Создание таблицы и вставка данных
def insert():
      
    
    client.execute("""INSERT INTO default.local_data_dist
SELECT
    number AS id,
    today() - interval (number % 30) day AS event_date,
    concat('user_', toString(number % 100)) AS user_id,
    rand() / 1000000 AS value
FROM numbers(1000);""")
    print("Данные вставлены")

# Read: Чтение данных
def read_data():
    result = client.execute('SELECT count() FROM default.local_data_dist')
    print("Read data:", result)

# Update: Обновление данных
def update_data():
    client.execute('ALTER TABLE example_table UPDATE age = 26 WHERE id = 1')
    print("Data updated")

# Delete: Удаление данных
def delete_data():
    client.execute('TRUNCATE TABLE default.local_data_dist')
    print("Data deleted")

# Основной блок
if __name__ == '__main__':
    # insert()
    # read_data()
    # update_data()
    # delete_data()
    # read_data()
    
    url = 'http://localhost:8125/'
    response = requests.post(url, data="SELECT count() FROM default.local_data")
    print(response.text)

