import json
import sys
import time
from threading import Thread

from publisher_subscriber import RabbitMQ

hostname = 'localhost'


def publish(queue, num, n):
    vec = [0 for x in range(n)]
    queue.connect(client='publisher')
    while True:
        for i in range(3):
            vec[num] += 1
            message = json.dumps({num: vec})
            queue.publish(message, True, pnum=num)
        time.sleep(2)


def subscribe(queue, num):
    queue.connect(client='subscriber')
    time.sleep(3)
    queue.consumer(callback=None, snum=num)


def main():
    n = int(sys.argv[1])
    num = int(sys.argv[2])
    if num >= n:
        print("error")
        return -1
    queue = RabbitMQ(hostname, '', N=n, type='correct')
    thread1 = Thread(target=publish, args=(queue, num, n))
    thread2 = Thread(target=subscribe, args=(queue, num))
    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()


if __name__ == '__main__':
    main()
