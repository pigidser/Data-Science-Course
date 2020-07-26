import pika
import pickle
import numpy as np
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
fh = logging.FileHandler(u"./logs/model.log", "w")
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
log.addHandler(ch)
log.addHandler(fh)

try:
    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open(dir_path+'/model.pkl', 'rb') as pkl_file:
        regressor = pickle.load(pkl_file)
except FileNotFoundError as err:
    log.error('Not found a file (model.pkl) with the trained model')
    import sys
    sys.exit()

while True:
    try:
        time.sleep(5)
        # connect to server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        channel = connection.channel()
        # create queues
        channel.queue_declare(queue='X_sample')
        channel.queue_declare(queue='y_pred')
    except (pika.exceptions.AMQPConnectionError):
        log.warning(u'Waiting for the RabbitMQ server is started')
    except (Exception) as err:
        log.error(err)
    else:
        log.info(u'The connection has been established')
        break

try:
    def callback_predict(channel, method_frame, header_frame, body):
        log.info(f'Received feature vector {body}')
        X_sample = np.reshape(np.array(json.loads(body)),(1,-1))
        y_pred = regressor.predict(X_sample)[0]
        channel.basic_publish(exchange='',
                            routing_key='y_pred',
                            body=json.dumps(y_pred))
        log.info('Message with y_pred value sent to queue')
    channel.basic_consume(
        queue='X_sample', on_message_callback=callback_predict, auto_ack=True)
    log.info('...waiting messages, press CTRL+C to exit')
    channel.start_consuming()
except KeyboardInterrupt:
    import sys
    sys.exit()
except (Exception) as err:
    log.info('Unhandled error:',err)