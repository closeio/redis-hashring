import binascii
import collections
import socket
import time
import random
import operator
import os

# Amount of points on the ring. Must not be higher than 2**32 because we're
# using CRC32 to compute the checksum.
RING_SIZE = 2 ** 32

# Default amount of replicas per node
RING_REPLICAS = 16

# How often to update a node's heartbeat
POLL_INTERVAL = 10

# After how much time a node is considered to be dead
NODE_TIMEOUT = 60

# How often expired nodes are cleaned up from the ring
CLEANUP_INTERVAL = 120


def _decode(data):
    # Compatibility with different redis-py decode_responses settings
    if isinstance(data, bytes):
        return data.decode()
    else:
        return data


class RingNode(object):
    """
    Represents a node in a Redis hash ring. Each node may have multiple
    replicas on the ring for more balanced hashing.

    The ring is stored as follows in Redis:

    ZSET <key>
    Represents the ring in Redis. The keys of this ZSET represent
    "start:replica_name", where start is the start of the range for which
    the replica is responsible.

    CHANNEL <key>
    Represents a pubsub channel in Redis which receives a message every
    time the ring structure has changed.

    Simple usage example for a distributed gevent-based application:

    ```
    node = RingNode(redis, key)
    node.gevent_start()

    while is_running:
        # Only process items this node is reponsible for.
        items = [item for item in get_items() if node.contains(item)]
        process_items(items)

    node.gevent_stop()
    ```
    """

    def __init__(self, conn, key, n_replicas=RING_REPLICAS):
        """
        Initializes a Redis hash ring node, given the Redis connection, a key
        and the number of replicas.
        """

        self.conn = conn
        self.key = key

        host = socket.gethostname()
        pid = os.getpid()
        # Create unique identifiers for the replicas
        self.replicas = [
            (
                random.randrange(2 ** 32),
                '{host}:{pid}:{rand}'.format(
                    host=host, pid=pid, rand=binascii.hexlify(os.urandom(4)).decode()
                ),
            )
            for n in range(n_replicas)
        ]

        # List of tuples of ranges this node is responsible for, where a tuple
        # (a, b) includes any N matching a <= N < b.
        self.ranges = []

    def _fetch(self):
        """
        Internal helper that fetches the ring from Redis, including only active
        nodes/replicas. Returns a list of tuples (start, replica) (see
        _fetch_all docs for more details).
        """
        now = time.time()
        expiry_time = now - NODE_TIMEOUT

        data = self.conn.zrangebyscore(self.key, expiry_time, 'INF')

        ring = []

        for node_data in data:
            start, replica = _decode(node_data).split(':', 1)
            ring.append((int(start), replica))

        ring = sorted(ring, key=operator.itemgetter(0))

        return ring

    def _fetch_all(self):
        """
        Internal helper that fetches the ring from Redis, including any
        inactive nodes/replicas. Returns a list of tuples (start, replica,
        heartbeat, expired), where
        * start: start of the range for which the replica is responsible
        * replica: name of the replica
        * heartbeat: unix time stamp of the last heartbeat
        * expired: boolean denoting whether this replica is inactive
        """

        now = time.time()
        expiry_time = now - NODE_TIMEOUT

        data = self.conn.zrange(self.key, 0, -1, withscores=True)

        ring = []

        for node_data, heartbeat in data:
            start, replica = _decode(node_data).split(':', 1)
            ring.append((int(start), replica, heartbeat, heartbeat < expiry_time))

        ring = sorted(ring, key=operator.itemgetter(0))

        return ring

    def debug_print(self):
        """
        Prints the ring for debugging purposes.
        """

        ring = self._fetch_all()

        print('Hash ring "{key}" replicas:'.format(key=self.key))

        now = time.time()
        n_replicas = len(ring)
        if ring:
            print('{:10} {:6} {:7} {}'.format('Start', 'Range', 'Delay', 'Node'))
        else:
            print('(no replicas)')

        nodes = collections.defaultdict(list)

        for n, (start, replica, heartbeat, expired) in enumerate(ring):
            hostname, pid, rnd = replica.split(':')
            node = ':'.join([hostname, pid])

            abs_size = (ring[(n + 1) % n_replicas][0] - ring[n][0]) % RING_SIZE
            size = 100.0 / RING_SIZE * abs_size
            delay = int(now - heartbeat)

            nodes[node].append((hostname, pid, abs_size, delay, expired))

            print(
                '{start:10} {size:5.2f}% {delay:6}s {replica}{extra}'.format(
                    start=start,
                    replica=replica,
                    delay=delay,
                    size=size,
                    extra=' (EXPIRED)' if expired else '',
                )
            )

        print()
        print('Hash ring "{key}" nodes:'.format(key=self.key))

        if nodes:
            print(
                '{:8} {:8} {:7} {:20} {:5}'.format(
                    'Range', 'Replicas', 'Delay', 'Hostname', 'PID'
                )
            )
        else:
            print('(no nodes)')

        for k, v in nodes.items():
            hostname, pid = v[0][0], v[0][1]
            abs_size = sum(replica[2] for replica in v)
            size = 100.0 / RING_SIZE * abs_size
            delay = max(replica[3] for replica in v)
            expired = any(replica[4] for replica in v)
            count = len(v)
            print(
                '{size:5.2f}% {count:8} {delay:6}s {hostname:20} {pid:5}{extra}'.format(
                    start=start,
                    count=count,
                    hostname=hostname,
                    pid=pid,
                    delay=delay,
                    size=size,
                    extra=' (EXPIRED)' if expired else '',
                )
            )

    def heartbeat(self):
        """
        Add/update the node in Redis. Needs to be called regularly by the
        client.
        """
        pipeline = self.conn.pipeline()
        now = time.time()
        for replica in self.replicas:
            pipeline.zadd(
                self.key,
                {
                    '{start}:{name}'.format(start=replica[0], name=replica[1]): now,
                },
            )
        ret = pipeline.execute()

        # Only notify the other nodes if we're not in the ring yet.
        if any(ret):
            self._notify()

    def remove(self):
        """
        Call this to remove the node/replicas from the ring.
        """
        pipeline = self.conn.pipeline()
        for replica in self.replicas:
            pipeline.zrem(
                self.key, '{start}:{name}'.format(start=replica[0], name=replica[1])
            )
        pipeline.execute()
        self._notify()

    def _notify(self):
        """
        Internal helper method which publishes an update to the ring's
        activity channel.
        """
        # Publish a dummy message on the activity channel
        self.conn.publish(self.key, '*')

    def cleanup(self):
        """
        Removes expired nodes/replicas from the ring.
        """
        now = time.time()
        expired = now - NODE_TIMEOUT
        if self.conn.zremrangebyscore(self.key, 0, expired):
            self._notify()

    def update(self):
        """
        Fetches the updated ring from Redis and updates the current ranges.
        """
        ring = self._fetch()
        n_replicas = len(ring)
        replica_set = set([r[1] for r in self.replicas])
        self.ranges = []
        for n, (start, replica) in enumerate(ring):
            if replica in replica_set:
                end = ring[(n + 1) % n_replicas][0] % RING_SIZE
                if start < end:
                    self.ranges.append((start, end))
                elif end < start:
                    self.ranges.append((start, RING_SIZE))
                    self.ranges.append((0, end))
                else:
                    self.ranges.append((0, RING_SIZE))

    def contains(self, key):
        """
        Returns a boolean indicating if this node is responsible for handling
        the given key.
        """
        return self.contains_ring_point(self.key_as_ring_point(key))

    def key_as_ring_point(self, key):
        """Turn a key into a point on a hash ring."""
        return binascii.crc32(key.encode()) % RING_SIZE

    def contains_ring_point(self, n):
        """
        Returns a boolean indicating if this node is responsible for handling
        the given point on a hash ring.
        """
        for start, end in self.ranges:
            if start <= n < end:
                return True
        return False

    def poll(self, as_greenlet=False):
        """
        Main loop which maintains the node in the hash ring. Can be run in a
        greenlet or separate thread. This takes care of:

        * Updating the heartbeat
        * Checking for ring updates
        * Cleaning up expired nodes periodically
        """

        pubsub = self.conn.pubsub()
        if as_greenlet:
            import gevent.socket
            pubsub.connection._sock = gevent.socket.socket(
                fileno=pubsub.connection._sock.fileno()
            )

        pubsub.subscribe(self.key)

        last_heartbeat = time.time()
        self.heartbeat()

        last_cleanup = time.time()
        self.cleanup()

        try:
            while True:
                timeout = max(0, POLL_INTERVAL - (time.time() - last_heartbeat))
                message = pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=timeout
                )
                if message:
                    # Pull remaining messages off of channel.
                    while pubsub.get_message():
                        pass
                    self.update()

                last_heartbeat = time.time()
                self.heartbeat()

                now = time.time()
                if now - last_cleanup > CLEANUP_INTERVAL:
                    last_cleanup = now
                    self.cleanup()
        finally:
            pubsub.close()

    def gevent_start(self):
        """
        Helper method to start the node for gevent-based applications.
        """
        import gevent

        self._poller_greenlet = gevent.spawn(self.poll, as_greenlet=True)
        self.heartbeat()
        self.update()

    def gevent_stop(self):
        """
        Helper method to stop the node for gevent-based applications.
        """
        import gevent

        gevent.kill(self._poller_greenlet)
        self.remove()
