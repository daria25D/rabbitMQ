import sys
import time

from publisher_subscriber import RabbitMQ

hostname = 'localhost'


def main():
    n = int(sys.argv[1])
    i = int(sys.argv[2])
    queue = RabbitMQ(hostname, '', N=n, type='incorrect')
    queue.connect(client='subscriber')
    queue.consumer(callback=None, snum=i)


if __name__ == '__main__':
    main()
