import argparse
import gevent
import logging
import os
from redis import Redis
from redis_hashring import RingNode

if __name__ == '__main__':
    parser = argparse.ArgumentParser('Hash ring example.')
    parser.add_argument('--print', action='store_true', dest='p', help='Print the hash ring.')

    args = parser.parse_args()
    N_KEYS = 100

    pid = os.getpid()

    redis = Redis()
    node = RingNode(redis, 'ring')

    if args.p:
        node.debug_print()

    else:

        node.gevent_start()

        logging.basicConfig(level=logging.DEBUG)

        try:

            while True:
                keys = [key for key in range(N_KEYS) if node.contains(str(key))]
                logging.info('PID %d, %d keys (%s)', pid, len(keys), repr(keys))
                gevent.sleep(2)

        except KeyboardInterrupt:
            pass

        node.gevent_stop()
