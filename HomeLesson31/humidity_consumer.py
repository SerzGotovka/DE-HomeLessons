import json
import psycopg2
from confluent_kafka import Consumer

# Настройки Kafka
consumer_conf = {
    'bootstrap.servers': 'localhost:9094',
    'group.id': 'humidity_consumer_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['topic_humidity'])

# Настройки PostgreSQL
db_config = {
    'host': 'localhost',
    'database': 'testdb',
    'user': 'admin',
    'password': 'secret',
    'port': '5433'
}

def create_table():
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS humidity_readings (
                    id SERIAL PRIMARY KEY,
                    value FLOAT NOT NULL,
                    timestamp TIMESTAMP NOT NULL
                )
            """)
            conn.commit()

create_table()

def save_to_db(data):
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO humidity_readings (value, timestamp)
                VALUES (%s, %s)
            """, (data['value'], data['timestamp']))
            conn.commit()

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Ошибка потребителя: {msg.error()}")
            continue

        data = json.loads(msg.value().decode('utf-8'))
        print(f"Получено сообщение о влажности: {data}")
        save_to_db(data)

        # Ручной коммит
        consumer.commit(msg)
except KeyboardInterrupt:
    print("Прерывание работы humidity consumer.")
finally:
    consumer.close()