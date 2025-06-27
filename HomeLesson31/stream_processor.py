import json
from confluent_kafka import Consumer, Producer

# Конфигурация Kafka
consumer_conf = {
    'bootstrap.servers': 'localhost:9094',
    'group.id': 'stream_processor_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['raw_data'])

producer_conf = {
    'bootstrap.servers': 'localhost:9094'
}

producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err:
        print(f'Ошибка доставки: {err}')
    else:
        print(f'Сообщение отправлено в {msg.topic()}')

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Ошибка потребителя: {msg.error()}")
            continue

        data = json.loads(msg.value().decode('utf-8'))
        topic = 'topic_temperature' if data['type'] == 'temperature' else 'topic_humidity'

        producer.produce(topic, value=msg.value(), callback=delivery_report)
        producer.poll(0)

        # Ручной коммит
        consumer.commit(msg)
except KeyboardInterrupt:
    print("Прерывание работы stream processor.")
finally:
    consumer.close()
    producer.flush()




















# from confluent_kafka import Consumer, Producer
# import json
# from producer import admin

# # Настройки Kafka
# # bootstrap_servers = 'localhost:9092'
# # input_topic = 'raw_data'
# # output_topics = {
# #     'temperature': 'topic_temperature',
# #     'humidity': 'topic_humidity'
# # }

# conf = {
#     'bootstrap.servers': 'localhost:9094',
#     'group.id': 'sensor_consumer_group',
#     'auto.offset.reset': 'earliest'     """Что делает: Определяет, с какого места читать сообщения при отсутствии сохранённого оффсета
#                                             (например, при первом запуске или сбросе).
#                                             'earliest': Начать чтение с самого первого сообщения в топике.
#                                             'latest': Читать только новые сообщения (игнорировать старые).
#                                             Зачем: Контролировать, как потребитель обрабатывает исторические данные."""
# }

# consumer = Consumer(conf)           #Создаёт экземпляр Kafka-потребителя с заданными настройками.
# consumer.subscribe(['sensor_data']) #Подписывает потребитель на топик Kafka с названием sensor_data.

# # Создаем consumer
# consumer = Consumer(
#     input_topic,
#     bootstrap_servers=bootstrap_servers,
#     value_deserializer=lambda m: json.loads(m.decode('utf-8'))
# )

# # Создаем producer
# producer = Producer(
#     bootstrap_servers=bootstrap_servers,
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

# try:
#     for message in consumer:
#         data = message.value
#         msg_type = data['type']
#         target_topic = output_topics.get(msg_type)
#         if target_topic:
#             print(f"Перенаправление сообщения в топик {target_topic}: {data}")
#             producer.send(target_topic, value=data)
#         else:
#             print(f"Неизвестный тип сообщения: {msg_type}")
# except KeyboardInterrupt:
#     print("Прерывание работы stream processor.")
# finally:
#     consumer.close()
#     producer.close()