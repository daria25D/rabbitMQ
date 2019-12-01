import sys, json, logging, time, random
import pika

hostname = 'localhost'
logging.getLogger('pika').setLevel(logging.ERROR)

random.seed()


class RabbitMQ(object):
    c_queue =[]

    def __init__(self, hostname, queue, exchange=''):
        self.hostname = hostname
        self.queue = queue
        self.exchange = 'logs'
        self.connection = None
        self.channel = None

        self.connect()

    def connect(self):
        sys.stdout.write('\nOpening RabbitMQ connection via %s for %s' \
                         % (self.hostname, self.queue))
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.hostname))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange, exchange_type='fanout')
        result = self.channel.queue_declare(queue=self.queue, exclusive=True)
        self.queue = result.method.queue
        self.channel.queue_bind(exchange=self.exchange, queue=self.queue)

    def __del__(self):
        sys.stdout.write('\nClosing RabbitMQ connection via %s for %s' \
                         % (self.hostname, self.queue))
        self.connection.close()

        # producer

    def publish(self, messages, display_output=False):
        for message in messages:
            self.channel.basic_publish(exchange=self.exchange, routing_key="",
                                       body=message, properties=pika.BasicProperties(delivery_mode=2))
            if display_output:
                sys.stdout.write('\n[x] Sent! %r' % (message,))
            # self.connection.sleep(1)

    def print_queue(self, ch, method, body, display_output=True):
        for body in self.c_queue:
            message = json.loads(body)
            if display_output:
                sys.stdout.write('\n[x] Received %r' % (message))
        self.c_queue = []

    def callback(self, ch, method, properties, body, display_output=True):
        # probability = random.uniform(0.0, 1.0) # broken
        probability = 0.0 # correct
        if probability > 0.5:
            self.c_queue.append(body)
            if len(self.c_queue) > 3:
                self.print_queue(ch, method, body, display_output)
        else:
            message = json.loads(body)
            if display_output:
                sys.stdout.write('\n[x] Received %r' % (message))

        ch.basic_ack(delivery_tag=method.delivery_tag)


    def flush(self):
        self.channel.queue_delete(queue=self.queue)
        self.connect()

        # consumer

    def consumer(self, callback=None):
        if callback is None:
            callback = self.callback
        sys.stdout.write('\n[*] Waiting for messages. To exit press CTRL+C\n\n')

        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(queue=self.queue, on_message_callback=callback)
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print('RabbitMQ -> keyboard interruption!!!!!')
            self.channel.stop_consuming()
        finally:
            self.connection.close()


def flush(queue):
    queue.flush()
    queue = RabbitMQ(hostname, queue.queue)


def test_producer(queue=None):
    if queue is None:
        queue = RabbitMQ(hostname, 'test')
    messages = [json.dumps({i: 'message #' + str(i)}) for i in range(2)]
    queue.publish(messages, display_output=True)


def test_consumer(queue=None):
    if queue is None:
        queue = RabbitMQ(hostname, 'test')
    queue.consumer()  # callback)


def test():
    queue = RabbitMQ(hostname, 'test1')
    test_producer(queue)
    time.sleep(1)
    test_consumer(queue)
    queue.__del__()


# test()


