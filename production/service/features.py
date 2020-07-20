import numpy as np
from sklearn.datasets import load_diabetes
import pika
import json

X, y = load_diabetes(return_X_y=True)
random_row = np.random.randint(0, X.shape[0]-1)
# print(X[random_row][:4])

# Подключение к серверу на локальном хосте:
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Создадим очередь, с которой будем работать:
channel.queue_declare(queue='Features')
channel.queue_declare(queue='y_true')

# Опубликуем сообщение
# exchange определяет, в какую очередь отправляется сообщение, 
# параметр routing_key указывает имя очереди, 
# параметр body тело самого сообщения, 
channel.basic_publish(exchange='',
                      routing_key='Features',
                      body=json.dumps(list(X[random_row][:4])))
print('Message with features has been sent to queue')
channel.basic_publish(exchange='',
                      routing_key='y_true',
                      body=json.dumps(y[random_row]))
print('Message with an correct answer has been sent to a queue')
# Закроем подключение 
connection.close()