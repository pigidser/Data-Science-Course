import numpy as np
from sklearn.datasets import load_diabetes
import pika
import json
import os, sys, time
import logging

# Set up logging
os.makedirs('logs', exist_ok=True)
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
formatter = logging.Formatter(fmt="%(asctime)s %(levelname)s: %(message)s", 
                          datefmt="%Y-%m-%d - %H:%M:%S")
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
fh = logging.FileHandler(u"./logs/features.log", "w")
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
log.addHandler(ch)
log.addHandler(fh)

X, y = load_diabetes(return_X_y=True)

while True:
    try:
        time.sleep(5)
        # connect to server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        channel = connection.channel()
        # create queues
        channel.queue_declare(queue='X_sample')
        channel.queue_declare(queue='y_true')
    except (pika.exceptions.AMQPConnectionError):
        log.warning(u'Waiting for the RabbitMQ server is started')
    except (Exception) as err:
        log.error(err)
    else:
        log.info(u'The connection has been established')
        break

while True:
    try:
        # get sample X and y from the dataset
        random_row = np.random.randint(0, X.shape[0]-1)
        X_sample = list(X[random_row])
        y_true = y[random_row]
        # Publish messages
        channel.basic_publish(exchange='',
                            routing_key='X_sample',
                            body=json.dumps(X_sample))
        log.info(u'X_sample was sent')
        channel.basic_publish(exchange='',
                            routing_key='y_true',
                            body=json.dumps(y_true))
        log.info(u'y_true was sent')
        # connection.close()
        time.sleep(3)
    except KeyboardInterrupt:
        import sys
        sys.exit()
    except (Exception) as err:
        log.error(u'Unhandled error:',err)