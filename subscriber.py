import json, time
import pika
from publisher_subscriber import RabbitMQ

hostname = 'localhost'


def subscribe():
    queue = RabbitMQ(hostname, '')
    while True:
        time.sleep(5)
        queue.consumer()


subscribe()