import pika
import pickle
import numpy as np
import json
import os

dir_path = os.path.dirname(os.path.realpath(__file__))

with open(dir_path+'/hw1.pkl', 'rb') as pkl_file:
    regressor = pickle.load(pkl_file)

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='Features')

def callback_features(channel, method_frame, header_frame, body):
    print(f'I got feature vector {body}')
    features = np.reshape(np.array(json.loads(body)),(1,-1))
    y_pred = regressor.predict(features)
    print(y_pred)
    channel.queue_declare(queue='y_pred')
    channel.basic_publish(exchange='',
                        routing_key='y_pred',
                        body=json.dumps(list(y_pred)))
    print('Message with y_pred value has been sent to queue')

def callback_y_true(channel, method_frame, header_frame, body):
    print(f'I got y_true value {body}')

# on_message_callback показывает какую функцию вызвать при получении сообщения
channel.basic_consume(
    queue='Features', on_message_callback=callback_features, auto_ack=True)

print('...waiting messages, press CTRL+C to exit')

channel.start_consuming()