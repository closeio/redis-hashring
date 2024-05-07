import argparse
import logging
import os
import sys
import time

import redis
from redis_hashring import RingNode

N_KEYS = 100

logging.basicConfig(level=logging.DEBUG)


def _parse_arguments():
    parser = argparse.ArgumentParser('Hash ring example.', add_help=False)
    parser.add_argument('--host', '-h', default='localhost',
                        help='Redis hostname.')
    parser.add_argument('--port', '-p', type=int, default=6379,
                        help='Redis port.')
    parser.add_argument('--print', action='store_true', dest='print',
                        help='Print the hash ring.')
    parser.add_argument('--help', action='help',
                        default=argparse.SUPPRESS,
                        help='Show this help message and exit.')
    return parser.parse_args()


if __name__ == '__main__':
    args = _parse_arguments()

    print(f'Attempting to connect to Redis at {args.host}:{args.port}.')
    r = redis.Redis(args.host, args.port)
    try:
        r.ping()
    except redis.exceptions.ConnectionError:
        print('Failed to connect to Redis.')
        sys.exit(1)

    node = RingNode(r, 'ring')

    if args.print:
        node.debug_print()
        sys.exit()

    pid = os.getpid()

    node.start()

    try:
        while True:
            keys = [key for key in range(N_KEYS) if node.contains(str(key))]
            logging.info('PID %d, %d keys (%s)', pid, len(keys), repr(keys))
            time.sleep(2)
    except KeyboardInterrupt:
        pass

    node.stop()
