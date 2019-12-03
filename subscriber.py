import sys
import time

from publisher_subscriber import RabbitMQ

hostname = 'localhost'


def subscribe():
    queue = RabbitMQ(hostname, '')
    while True:
        time.sleep(5)
        queue.consumer()


def main():
    n = int(sys.argv[1])
    i = int(sys.argv[2])
    queue = (RabbitMQ(hostname, '', N=n))
    queue.connect(client='subscriber')
    queue.consumer(callback=None, snum=i)


if __name__ == '__main__':
    main()
