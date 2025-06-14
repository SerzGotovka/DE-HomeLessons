# from clickhouse_driver import Client
import clickhouse_connect

client_01 = clickhouse_connect.get_client(host='localhost',
                                       port=8124, 
                                       username='default', 
                                       password='')

client_02 = clickhouse_connect.get_client(host='localhost',
                                       port=8125, 
                                       username='default', 
                                       password='')
# Подключение к серверу
client_master = clickhouse_connect.get_client(host='localhost',
                                       port=8123, 
                                       username='default', 
                                       password='')


# Функция для создания записи
def create_record():
    client_master.query(
            """INSERT INTO default.local_data_dist
        SELECT
            number AS id,
            today() - interval (number % 30) day AS event_date,
            concat('user_', toString(number % 100)) AS user_id,
            rand() / 1000000 AS value
        FROM numbers(1000);"""
    )

# Функция для чтения записей
def read_records():
    result = client_master.query("SELECT * FROM default.local_data")
    for i in result.result_rows:
        print(i)
    

# Функция для обновления записи
def update_record():
    client_02.query(
        "ALTER TABLE default.local_data UPDATE user_id = 'user_223213' WHERE id = 2"
    )

# Функция для удаления записи
def delete_record(id):
    client_02.query(
        "ALTER TABLE default.local_data DELETE WHERE id = %s", 
        (id,)
    )
    
# Пример использования
if __name__ == "__main__":
    # #Создание записей
    create_record()
  

    # #Чтение записей
    read_records()
    

    # #Обновление записи
    update_record()


    # # Удаление записи
    delete_record(2)

    # # Чтение записей после удаления
    # records = read_records()
    # print("Records after deletion:", records)
 