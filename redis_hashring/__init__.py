import binascii
import collections
import operator
import os
import random
import select
import socket
import threading
import time

# Amount of points on the ring. Must not be higher than 2**32 because we're
# using CRC32 to compute the checksum.
RING_SIZE = 2**32

# Default amount of replicas per node.
RING_REPLICAS = 16

# How often to update a node's heartbeat.
POLL_INTERVAL = 10

# After how much time a node is considered to be dead.
NODE_TIMEOUT = 60

# How often expired nodes are cleaned up from the ring.
CLEANUP_INTERVAL = 120


def _decode(data):
    # Compatibility with different redis-py `decode_responses` settings.
    if isinstance(data, bytes):
        return data.decode()
    else:
        return data


class RingNode(object):
    """
    A node in a Redis hash ring.

    Each node may have multiple replicas on the ring for more balanced hashing.

    The ring is stored as follows in Redis:

    ZSET <key>
    Represents the ring in Redis. The keys of this ZSET represent
    "start:replica_name", where start is the start of the range for which the
    replica is responsible.

    CHANNEL <key>
    Represents a pubsub channel in Redis which receives a message every time
    the ring structure has changed.

    Simple usage example for a distributed threads-based application:

    ```
    node = RingNode(redis, key)
    node.start()

    while is_running:
        # Only process items this node is responsible for. `item` should be an
        # object that can be encoded to bytes by calling `item.encode()` on it,
        # like a `str`.
        items = [item for item in get_items() if node.contains(item)]
        process_items(items)

    node.stop()
    ```

    Simple usage example for a distributed gevent-based application:

    ```
    node = RingNode(redis, key)
    node.gevent_start()

    while is_running:
        # Only process items this node is responsible for. `item` should be an
        # object that can be encoded to bytes by calling `item.encode()` on it,
        # like a `str`.
        items = [item for item in get_items() if node.contains(item)]
        process_items(items)

    node.gevent_stop()
    ```
    """

    def __init__(self, conn, key, n_replicas=RING_REPLICAS):
        """
        Initializes a Redis hash ring node.

        Args:
            conn: The Redis connection to use.
            key: A key to use for this node.
            n_replicas: Number of replicas this node should have on the ring.
        """
        self._polling_thread = None
        self._polling_greenlet = None
        self._stop_polling_fd_r = None
        self._stop_polling_fd_w = None

        self._conn = conn
        self._key = key
        host = socket.gethostname()
        pid = os.getpid()

        # Create unique identifiers for the replicas.
        self._replicas = [
            (
                random.randrange(2**32),
                "{host}:{pid}:{id_}".format(
                    host=host,
                    pid=pid,
                    id_=binascii.hexlify(os.urandom(4)).decode(),
                ),
            )
            for _ in range(n_replicas)
        ]

        # Number of nodes currently active in the ring.
        self._node_count = 0
        # List of tuples of ranges this node is responsible for, where a tuple
        # (a, b) includes any N matching a <= N < b.
        self._ranges = []

        self._select = select.select

    def _fetch_ring(self):
        """
        Fetch the ring from Redis.

        The fetched ring only includes active nodes. Returns a list of tuples
        (start, replica) (see _fetch_all docs for more details).
        """
        expiry_time = time.time() - NODE_TIMEOUT
        data = self._conn.zrangebyscore(self._key, expiry_time, "INF")

        ring = []
        for replica_data in data:
            start, replica = _decode(replica_data).split(":", 1)
            ring.append((int(start), replica))
        return sorted(ring, key=operator.itemgetter(0))

    def _fetch_ring_all(self):
        """
        Fetch the ring from Redis.

        The fetched ring will include inactive nodes. Returns a list of tuples
        (start, replica, heartbeat, expired), where:
        * start: start of the range for which the replica is responsible.
        * replica: name of the replica.
        * heartbeat: timestamp of the last heartbeat.
        * expired: boolean denoting whether this replica is inactive.
        """
        expiry_time = time.time() - NODE_TIMEOUT
        data = self._conn.zrange(self._key, 0, -1, withscores=True)

        ring = []
        for replica_data, heartbeat in data:
            start, replica = _decode(replica_data).split(":", 1)
            ring.append(
                (int(start), replica, heartbeat, heartbeat < expiry_time)
            )
        return sorted(ring, key=operator.itemgetter(0))

    def debug_print(self):
        """
        Prints the ring for debugging purposes.
        """
        ring = self._fetch_ring_all()

        print('Hash ring "{key}" replicas:'.format(key=self._key))

        now = time.time()

        n_replicas = len(ring)
        if ring:
            print(
                "{:10} {:6} {:7} {}".format("Start", "Range", "Delay", "Node")
            )
        else:
            print("(no replicas)")

        nodes = collections.defaultdict(list)

        for n, (start, replica, heartbeat, expired) in enumerate(ring):
            hostname, pid, _ = replica.split(":")
            node = ":".join([hostname, pid])

            abs_size = (ring[(n + 1) % n_replicas][0] - ring[n][0]) % RING_SIZE
            size = 100.0 / RING_SIZE * abs_size
            delay = int(now - heartbeat)
            expired_str = "(EXPIRED)" if expired else ""

            nodes[node].append((hostname, pid, abs_size, delay, expired))

            print(
                f"{start:10} {size:5.2f}% {delay:6}s {replica} {expired_str}"
            )

        print()
        print('Hash ring "{key}" nodes:'.format(key=self._key))

        if nodes:
            print(
                "{:8} {:8} {:7} {:20} {:5}".format(
                    "Range", "Replicas", "Delay", "Hostname", "PID"
                )
            )
        else:
            print("(no nodes)")

        for _, v in nodes.items():
            hostname, pid = v[0][0], v[0][1]
            abs_size = sum(replica[2] for replica in v)
            size = 100.0 / RING_SIZE * abs_size
            delay = max(replica[3] for replica in v)
            expired = any(replica[4] for replica in v)
            count = len(v)
            expired_str = "(EXPIRED)" if expired else ""
            print(
                f"{size:5.2f}% {count:8} {delay:6}s {hostname:20} {pid:5}"
                f" {expired_str}"
            )

    def heartbeat(self):
        """
        Add/update the node in Redis.

        Needs to be called regularly by the node.
        """
        pipeline = self._conn.pipeline()

        now = time.time()

        for replica in self._replicas:
            pipeline.zadd(self._key, {f"{replica[0]}:{replica[1]}": now})
        ret = pipeline.execute()

        # Only notify the other nodes if we're not in the ring yet.
        if any(ret):
            self._notify()

    def remove(self):
        """
        Remove the node from the ring.
        """
        pipeline = self._conn.pipeline()

        for replica in self._replicas:
            pipeline.zrem(self._key, f"{replica[0]}:{replica[1]}")
        pipeline.execute()

        # Make sure this node won't contain any items.
        self._node_count = 0
        self._ranges = []

        self._notify()

    def _notify(self):
        """
        Publish an update to the ring's activity channel.
        """
        self._conn.publish(self._key, "*")

    def cleanup(self):
        """
        Removes expired nodes from the ring.
        """
        expired = time.time() - NODE_TIMEOUT

        if self._conn.zremrangebyscore(self._key, 0, expired):
            self._notify()

    def update(self):
        """
        Fetches the updated ring from Redis and updates the current ranges.
        """
        ring = self._fetch_ring()
        nodes = set()
        n_replicas = len(ring)

        own_replicas = {r[1] for r in self._replicas}

        self._ranges = []
        for n, (start, replica) in enumerate(ring):
            host, pid, _ = replica.split(":")
            node = ":".join([host, pid])
            nodes.add(node)

            if replica in own_replicas:
                end = ring[(n + 1) % n_replicas][0] % RING_SIZE
                if start < end:
                    self._ranges.append((start, end))
                elif end < start:
                    self._ranges.append((start, RING_SIZE))
                    self._ranges.append((0, end))
                else:
                    self._ranges.append((0, RING_SIZE))

        self._node_count = len(nodes)

    def get_ranges(self):
        """
        Return the hash ring ranges that this node owns.
        """
        return self._ranges

    def get_node_count(self):
        """
        Return the number of active nodes in the ring.
        """
        return self._node_count

    def contains(self, key):
        """
        Check whether this node is responsible for the item.
        """
        return self._contains_ring_point(self.key_as_ring_point(key))

    def key_as_ring_point(self, key):
        """Turn a key into a point on a hash ring."""
        return binascii.crc32(key.encode()) % RING_SIZE

    def _contains_ring_point(self, n):
        """
        Check whether this node is responsible for the ring point.
        """
        for start, end in self._ranges:
            if start <= n < end:
                return True
        return False

    def poll(self):
        """
        Keep a node in the hash ring.

        This should be kept running for as long as the node needs to stay in
        the ring. Can be run in a separate thread or in a greenlet. This takes
        care of:
        * Updating the heartbeat.
        * Checking for ring updates.
        * Cleaning up expired nodes periodically.
        """
        pubsub = self._conn.pubsub()
        pubsub.subscribe(self._key)
        pubsub_fd = pubsub.connection._sock.fileno()

        last_heartbeat = time.time()
        self.heartbeat()

        last_cleanup = time.time()
        self.cleanup()

        self._stop_polling_fd_r, self._stop_polling_fd_w = os.pipe()

        try:
            while True:
                # Since Redis' `listen` method blocks, we use `select` to
                # inspect the underlying socket to see if there is activity.
                timeout = max(
                    0.0, POLL_INTERVAL - (time.time() - last_heartbeat)
                )
                r, _, _ = self._select(
                    [self._stop_polling_fd_r, pubsub_fd], [], [], timeout
                )

                if self._stop_polling_fd_r in r:
                    os.close(self._stop_polling_fd_r)
                    os.close(self._stop_polling_fd_w)
                    self._stop_polling_fd_r = None
                    self._stop_polling_fd_w = None
                    break

                if pubsub_fd in r:
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

    def start(self):
        """
        Start the node for threads-based applications.
        """
        self._polling_thread = threading.Thread(target=self.poll, daemon=True)
        self._polling_thread.start()

    def stop(self):
        """
        Stop the node for threads-based applications.
        """
        if self._polling_thread:
            while not self._stop_polling_fd_w:
                # Let's give the thread some time to create the fd.
                time.sleep(0.1)
            os.write(self._stop_polling_fd_w, b"1")
            self._polling_thread.join()
            self._polling_thread = None
        self.remove()

    def gevent_start(self):
        """
        Start the node for gevent-based applications.
        """
        import gevent
        import gevent.select

        self._select = gevent.select.select
        self._polling_greenlet = gevent.spawn(self.poll)
        self.heartbeat()
        self.update()

    def gevent_stop(self):
        """
        Stop the node for gevent-based applications.
        """
        if self._polling_greenlet:
            while not self._stop_polling_fd_w:
                # Let's give the greenlet some time to create the fd.
                time.sleep(0.1)
            os.write(self._stop_polling_fd_w, b"1")
            self._polling_greenlet.join()
            self._polling_greenlet = None
        self.remove()
        self._select = select.select
