import pika
import pickle
import numpy as np
import json
import os

dir_path = os.path.dirname(os.path.realpath(__file__))

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='y_pred')
channel.queue_declare(queue='y_true')

def callback_y_pred(channel, method_frame, header_frame, body):
    print(f'From queue {method_frame.routing_key} got y_pred: {json.loads(body)}')

def callback_y_true(channel, method_frame, header_frame, body):
    print(f'Got y_true: {body}')

# on_message_callback показывает какую функцию вызвать при получении сообщения
channel.basic_consume(
    queue='y_pred', on_message_callback=callback_y_pred, auto_ack=True)
channel.basic_consume(
    queue='y_true', on_message_callback=callback_y_true, auto_ack=True)
print('...waiting messages, press CTRL+C to exit')

channel.start_consuming()