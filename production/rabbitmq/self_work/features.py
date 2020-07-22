import numpy as np
from sklearn.datasets import load_diabetes
import pika
import json
import time

X, y = load_diabetes(return_X_y=True)

# Connect to the local server:
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# create queues
channel.queue_declare(queue='features_and_y_true')

while 1==1:
    time.sleep(5)
    random_row = np.random.randint(0, X.shape[0]-1)
    sample = list(X[random_row])
    sample.append(y[random_row])
    # Publish messages
    channel.basic_publish(exchange='',
                        routing_key='features_and_y_true',
                        body=json.dumps(sample))
    print('Message was added to the queue')

# Close connection
connection.close()