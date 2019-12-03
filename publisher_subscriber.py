import json
import logging
import random
import sys

import pika

hostname = 'localhost'
logging.getLogger('pika').setLevel(logging.ERROR)

random.seed()


class RabbitMQ(object):
    c_queue = []

    def __init__(self, hostname, queue, exchange='', N=2, type='correct'):
        self.hostname = hostname
        self.queue = queue
        self.exchange = 'logs'
        self.connection = None
        self.pub_channel = None
        self.sub_channel = None
        self.vec = None
        self.snum = 0
        self.publishers = []
        self.pnum = -1
        self.N = N
        self.type_correctness = type
        # self.connect()

    def connect(self, client):
        if client == 'publisher':
            sys.stdout.write('\nOpening RabbitMQ connection via %s for %s' \
                             % (self.hostname, self.queue))
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.hostname))
            self.pub_channel = self.connection.channel()
            self.pub_channel.exchange_declare(exchange=self.exchange, exchange_type='fanout')
            result = self.pub_channel.queue_declare(queue=self.queue)
            self.queue = result.method.queue
            self.pub_channel.queue_bind(exchange=self.exchange, queue=self.queue)
        elif client == 'subscriber':
            sys.stdout.write('\nOpening RabbitMQ connection via %s for %s' \
                             % (self.hostname, self.queue))
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.hostname))
            self.sub_channel = self.connection.channel()
            self.sub_channel.exchange_declare(exchange=self.exchange, exchange_type='fanout')
            result = self.sub_channel.queue_declare(queue=self.queue)
            self.queue = result.method.queue
            self.sub_channel.queue_bind(exchange=self.exchange, queue=self.queue)

    def __del__(self):
        sys.stdout.write('\nClosing RabbitMQ connection via %s for %s' \
                         % (self.hostname, self.queue))
        self.connection.close()

        # producer

    def publish(self, message, display_output=False, pnum=1):
        if self.vec is None:
            self.vec = [0 for i in range(self.N)]
        self.publishers.append(pnum)
        self.pnum = pnum
        self.vec[pnum] += 1
        self.pub_channel.basic_publish(exchange=self.exchange, routing_key="",
                                       body=message, properties=pika.BasicProperties(delivery_mode=2))
        if display_output:
            sys.stdout.write('\n[p] Sent! %r' % (message,))

    def print_queue(self, display_output=True):
        for m in self.c_queue:
            if display_output:
                sys.stdout.write('\n[s] Received %r' % (m))
        self.c_queue = []

    def callback(self, ch, method, properties, body, display_output=True):
        message = json.loads(body)
        sender_num = int(list(message.keys())[0])
        mvec = list(message.values())[0]
        flag = False
        if self.type_correctness == 'correct':
            if self.vec is None:
                self.vec = [mvec[i] for i in range(len(mvec))]
                self.vec[sender_num] -= 1
                # print(self.vec)
                flag = True
            elif self.vec[sender_num] == mvec[sender_num] - 1:
                for k in range(len(self.vec)):
                    if self.vec[k] < mvec[k] and k != sender_num:
                        flag = False
                    else:
                        flag = True
            if flag or self.pnum == sender_num:
                self.vec[sender_num] += 1
                if display_output:
                    sys.stdout.write('\n[s] Received %r' % (message))
        elif self.type_correctness == 'incorrect':
            probability = random.uniform(0, 1)
            if probability > 0.5:
                self.c_queue.append(message)
                if len(self.c_queue) > 3:
                    self.print_queue(display_output)
            else:
                if display_output:
                    sys.stdout.write('\n[s] Received %r' % (message))

        # ch.basic_ack(delivery_tag=method.delivery_tag)

    def flush(self):
        if self.queue:
            self.pub_channel.queue_delete(queue=self.queue)
        self.connect()

        # consumer

    def consumer(self, callback=None, snum=0):
        self.snum = snum
        if callback is None:
            callback = self.callback
        sys.stdout.write('\n[*] Waiting for messages. To exit press CTRL+C\n\n')

        try:
            self.sub_channel.basic_qos(prefetch_count=1)
            self.sub_channel.basic_consume(queue=self.queue, on_message_callback=callback, auto_ack=True)
            self.sub_channel.start_consuming()
        except KeyboardInterrupt:
            print('RabbitMQ -> keyboard interruption!!!!!')
            self.sub_channel.stop_consuming()
        finally:
            self.connection.close()
