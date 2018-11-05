
import time
from multiprocessing import Process, Lock, Pool
from random import randint

def test():
    batches = []

    for i in range(0, 128):
        batches.append(i)

    print len(batches), 'batches to process'
    p = Pool(4)
    print(p.map(process, batches))


def process(digit):
    print '-----------------------------------------------------'
    print 'Starting process', digit
    print '-----------------------------------------------------'

    for i in range(0, 5):
        # print digit, '----------', i, '---------'
        duration = randint(1, 3)
        # print 'duration', duration
        time.sleep(duration)

    print digit, 'done'

test()