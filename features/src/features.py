import pika
import numpy as np
import json
import time
from datetime import datetime
from sklearn.datasets import load_diabetes

# Инициализируем датасет и фиксируем случайное число
np.random.seed(42)
X, y = load_diabetes(return_X_y=True)

# Подключение к серверу RabbitMQ на локальном хосте
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Создаём очереди
channel.queue_declare(queue='y_true', durable=True)
channel.queue_declare(queue='features', durable=True)

try:
    while True:
        # Генерируем случайный индекс строки
        random_row = np.random.randint(0, X.shape[0])
        
        # Генерируем уникальный идентификатор на основе текущей метки времени
        message_id = datetime.timestamp(datetime.now())
        
        # Подготавливаем сообщения с идентификатором
        message_y_true = {
            'id': message_id,
            'body': y[random_row]
        }
        
        message_features = {
            'id': message_id,
            'body': list(X[random_row])
        }
        
        # Публикуем сообщение с правильным ответом в очередь y_true
        channel.basic_publish(
            exchange='',
            routing_key='y_true',
            body=json.dumps(message_y_true),
            properties=pika.BasicProperties(delivery_mode=2)  # Делает сообщение устойчивым
        )
        print(f"Сообщение с правильным ответом отправлено в очередь: {message_y_true}")
        
        # Публикуем сообщение с вектором признаков в очередь features
        channel.basic_publish(
            exchange='',
            routing_key='features',
            body=json.dumps(message_features),
            properties=pika.BasicProperties(delivery_mode=2)  # Делает сообщение устойчивым
        )
        print(f"Сообщение с вектором признаков отправлено в очередь: {message_features}")
        
        # Задержка перед отправкой следующего сообщения
        time.sleep(10)  # например, 10 секунд

except KeyboardInterrupt:
    print("Цикл прерван пользователем.")
finally:
    # Закрываем подключение, если цикл прерван
    connection.close()
    print("Подключение закрыто.")
