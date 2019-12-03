import sys, json, logging, time, random, sys
import pika

hostname = 'localhost'
logging.getLogger('pika').setLevel(logging.ERROR)

random.seed()


class RabbitMQ(object):
    c_queue = []

    def __init__(self, hostname, queue, exchange='', N=2):
        self.hostname = hostname
        self.queue = queue
        self.exchange = 'logs'
        self.connection = None
        self.channel = None
        self.vec = None
        self.snum = 0
        self.publishers = []
        self.pnum = -1
        self.N = N
        # self.connect()

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

    def publish(self, message, display_output=False, pnum=1):
        # for message in messages:
        if self.vec is None:
            self.vec = [0 for i in range(self.N)]
        self.publishers.append(pnum)
        self.pnum = pnum
        self.vec[pnum] += 1
        self.channel.basic_publish(exchange=self.exchange, routing_key="",
                                   body=message, properties=pika.BasicProperties(delivery_mode=2))
        if display_output:
            sys.stdout.write('\n[p] Sent! %r' % (message,))
            # self.connection.sleep(1)

    def print_queue(self, display_output=True):
        for m in self.c_queue:
            if display_output:
                sys.stdout.write('\n[s] Received %r' % (m))
        self.c_queue = []

    def callback(self, ch, method, properties, body, display_output=True):
        # probability = random.uniform(0.0, 1.0) # broken
        probability = 0.0 # correct
        message = json.loads(body)
        sender_num = int(list(message.keys())[0])
        mvec = list(message.values())[0]
        flag = False
        if self.vec is None:
            self.vec = [mvec[i] for i in range(len(mvec))]
            self.vec[sender_num] -= 1
            # print(self.vec)
            flag = True
        elif self.vec[sender_num] == mvec[sender_num]-1:
            for k in range(len(self.vec)):
                if self.vec[k] < mvec[k] and k != sender_num:
                    flag = False
                else:
                    flag = True
        # print(flag)
        if flag:
            self.vec[sender_num] += 1
            # print(self.vec)
            if display_output:
                sys.stdout.write('\n[s] Received %r' % (message))
            # ch.basic_ack(delivery_tag=method.delivery_tag)
        elif not flag and probability > 0.5:
            self.c_queue.append([message, sender_num, mvec])
            if len(self.c_queue) > 3:
                self.print_queue(True)
            # print("error " + self.c_queue)
            # for i in range(len(self.c_queue)):
            #     if self.vec[sender_num] == self.c_queue[i][2][sender_num]-1:
            #         for k in range(len(self.vec)):
            #             if self.vec[k] < self.c_queue[i][2][k] and k != sender_num:
            #                 flag = False
            #             else:
            #                 flag = True
            #     if flag:
            #         self.vec[sender_num] += 1
            #         print(self.vec)
            #         if display_output:
            #             sys.stdout.write('\n[s] Received %r' % (self.c_queue[i][0]))
            #         ch.basic_ack(delivery_tag=method.delivery_tag)


    def flush(self):
        if self.queue:
            self.channel.queue_delete(queue=self.queue)
        self.connect()

        # consumer

    def consumer(self, callback=None, snum=0):
        self.snum = snum
        if callback is None:
            callback = self.callback
        sys.stdout.write('\n[*] Waiting for messages. To exit press CTRL+C\n\n')

        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(queue=self.queue, on_message_callback=callback, auto_ack=True)
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print('RabbitMQ -> keyboard interruption!!!!!')
            self.channel.stop_consuming()
        finally:
            self.connection.close()




