import json, time
import pika
from publisher_subscriber import RabbitMQ

hostname = 'localhost'


def publish():
    count = 5
    queue = RabbitMQ(hostname, '')
    while True:
        messages = [json.dumps({i: 'message #' + str(i)}) for i in range(count-5, count)]
        queue.publish(messages, display_output=True)
        time.sleep(5)
        count += 5


publish()


