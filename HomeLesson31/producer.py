import random
import json
import time
from datetime import datetime, timezone
from confluent_kafka import Producer

# Конфигурация Kafka
conf = {
    'bootstrap.servers': 'localhost:9094'
}

producer = Producer(conf)

def generate_message():
    msg_type = random.choice(['temperature', 'humidity'])
    value = round(random.uniform(0, 100), 1)
    timestamp = datetime.now().isoformat()
    return json.dumps({
        "type": msg_type,
        "value": value,
        "timestamp": timestamp
    }).encode('utf-8')

def delivery_report(err, msg):
    if err:
        print(f'Ошибка доставки сообщения: {err}')
    else:
        print(f'[v] Отправил 1 сообщение, {datetime.now().isoformat()} в {msg.topic()} [{msg.partition()}]')

try:
    while True:
        msg = generate_message()
        producer.produce('raw_data', value=msg, callback=delivery_report)
        producer.poll(0)
        time.sleep(0.01)
except KeyboardInterrupt:
    print("Прерывание работы producer.")
finally:
    producer.flush()