from confluent_kafka import Consumer, KafkaError
import pandas as pd
import json
import time

# Конфигурация Kafka
kafka_conf = {
    'bootstrap.servers': 'localhost:9092',  # Укажите адрес и порт вашего брокера Kafka
    'group.id': 'iot_consumer_group',
    'auto.offset.reset': 'earliest'
}

# Создание потребителя Kafka
consumer = Consumer(kafka_conf)

# Топик для чтения сообщений
topic = 'iot_data_topic'

# Параметры вывода данных каждые 20 секунд
output_interval = 20
last_output_time = time.time()

# Пустые DataFrame для средних значений
average_by_type_df = pd.DataFrame(columns=['sensor_type', 'average_value'])
average_by_name_df = pd.DataFrame(columns=['sensor_name', 'average_value'])

# Функция обработки сообщения
def process_message(message):
    data = json.loads(message.value())
    return data

# Подписка на топик и обработка сообщений
consumer.subscribe([topic])
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        data = process_message(msg)
        
        # Обработка данных и обновление DataFrame
        # (здесь может быть логика для вычисления средних значений)

        # Вывод данных каждые 20 секунд
        current_time = time.time()
        if current_time - last_output_time >= output_interval:
            print("Average by Type:")
            print(average_by_type_df)
            print("\nAverage by Name:")
            print(average_by_name_df)
            print("\n------------------------")
            
            # Сброс данных
            average_by_type_df = pd.DataFrame(columns=['sensor_type', 'average_value'])
            average_by_name_df = pd.DataFrame(columns=['sensor_name', 'average_value'])
            
            last_output_time = current_time
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
