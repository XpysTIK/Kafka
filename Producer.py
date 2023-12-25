from confluent_kafka import Producer
import time
import json
import random

# Конфигурация Kafka
kafka_conf = {
    'bootstrap.servers': 'localhost:9092', 
}

# Количество датчиков
num_sensors = 5

# Функция генерации сообщения от датчика
def generate_sensor_data(sensor_id):
    sensor_type = random.choice(['temperature', 'pressure', 'humidity'])
    sensor_name = f"Sensor_{sensor_id}"
    sensor_value = round(random.uniform(10, 30), 2)  # Пример случайного значения

    message = {
        'timestamp': time.time(),
        'sensor_type': sensor_type,
        'sensor_name': sensor_name,
        'sensor_value': sensor_value,
    }

    return json.dumps(message)

# Функция отправки сообщения в топик
def produce_message(producer, topic, message):
    producer.produce(topic, key='key', value=message)
    producer.flush()

# Создание производителя Kafka
producer = Producer(kafka_conf)

# Топик для отправки сообщений
topic = 'iot_data_topic'

# Генерация и отправка сообщений от датчиков
try:
    while True:
        for sensor_id in range(1, num_sensors + 1):
            message = generate_sensor_data(sensor_id)
            produce_message(producer, topic, message)
            time.sleep(random.uniform(0.1, 1))  # Задержка между отправками
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
