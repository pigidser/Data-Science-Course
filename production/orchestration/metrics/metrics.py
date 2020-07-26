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
fh = logging.FileHandler(u"./logs/metrics.log", "w")
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
log.addHandler(ch)
log.addHandler(fh)

while True:
    try:
        time.sleep(5)
        # connect to server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        channel = connection.channel()
        # create queues
        channel.queue_declare(queue='y_true')
        channel.queue_declare(queue='y_pred')
    except (pika.exceptions.AMQPConnectionError):
        log.warning(u'Waiting for the RabbitMQ server is started')
    except (Exception) as err:
        log.error(err)
    else:
        log.info(u'The connection has been established')
        break

try:
    def callback_y_pred(channel, method_frame, header_frame, body):
        log.info(f'From queue {method_frame.routing_key} got {json.loads(body)}')
    def callback_y_true(channel, method_frame, header_frame, body):
        log.info(f'From queue {method_frame.routing_key} got {json.loads(body)}')
    channel.basic_consume(
        queue='y_pred', on_message_callback=callback_y_pred, auto_ack=True)
    channel.basic_consume(
        queue='y_true', on_message_callback=callback_y_true, auto_ack=True)
    log.info('...waiting messages, press CTRL+C to exit')
    channel.start_consuming()
except KeyboardInterrupt:
    import sys
    sys.exit()
except (Exception) as err:
    log.info('Unhandled error:',err)