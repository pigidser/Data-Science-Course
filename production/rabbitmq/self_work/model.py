import pika
import pickle
import numpy as np
import json
import os
from sklearn.metrics import mean_squared_error

dir_path = os.path.dirname(os.path.realpath(__file__))

with open(dir_path+'/model.pkl', 'rb') as pkl_file:
    model = pickle.load(pkl_file)

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='features_and_y_true')

y_true_list = []
y_pred_list = []

def callback_predict(channel, method_frame, header_frame, body):
    print(f'Got a message')
    sample = np.array(json.loads(body))[:10]
    y_true = np.array(json.loads(body))[10]
    features = np.reshape(sample,(1,-1))
    y_pred = model.predict(features)[0]
    y_true_list.append(y_true)
    y_pred_list.append(y_pred)
    print('y_true: {:.2f}, y_pred {:.2f}, RMSE: {:.2f}'.format(y_true, y_pred, mean_squared_error(y_true_list, y_pred_list , squared=False)))

# on_message_callback показывает какую функцию вызвать при получении сообщения
channel.basic_consume(
    queue='features_and_y_true', on_message_callback=callback_predict, auto_ack=True)

print('...waiting messages, press CTRL+C to exit')

channel.start_consuming()