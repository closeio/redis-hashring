import binascii
import collections
import enum
import operator
import os
import random
import select
import socket
import threading
import time

try:
    import xxhash
except ImportError:
    xxhash = None

# Amount of points on the ring. Must not be higher than 2**32.
RING_SIZE = 2**32

# Default amount of replicas per node.
RING_REPLICAS = 16


class HashAlgorithm(enum.Enum):
    CRC32 = "crc32"
    XXHASH = "xxhash"


# How often to update a node's heartbeat.
POLL_INTERVAL = 10

# After how much time a node is considered to be dead.
NODE_TIMEOUT = 60

# How often expired nodes are cleaned up from the ring.
CLEANUP_INTERVAL = 120

# The state of a node:
# * ranges: tuple of the ranges that the node owns, where a range (a, b)
#   includes any N matching a <= N < b.
# * node_count: number of nodes currently active in the ring.
_RingState = collections.namedtuple("_RingState", ["ranges", "node_count"])

_EMPTY_STATE = _RingState(ranges=(), node_count=0)


def _hash_with_xxhash(key):
    return xxhash.xxh32(key.encode()).intdigest() % RING_SIZE


def _hash_with_crc32(key):
    return binascii.crc32(key.encode()) % RING_SIZE


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

    Simple usage example:

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

    Using CRC-32 (if you need to support hashrings created before xxHash
    support was introduced):

    ```
    from redis_hashring import RingNode, HashAlgorithm

    node = RingNode(redis, key, hash_algorithm=HashAlgorithm.CRC32)
    node.start()
    ```

    As a context manager:

    ```
    with RingNode(redis, key) as node:
        while is_running:
            # Only process items this node is responsible for. `item` should be
            # an object that can be encoded to bytes by calling `item.encode()`
            # on it, like a `str`.
            items = [item for item in get_items() if node.contains(item)]
            process_items(items)
    ```
    """

    def __init__(
        self,
        conn,
        key,
        *,
        n_replicas=RING_REPLICAS,
        hash_algorithm=HashAlgorithm.XXHASH,
    ):
        """
        Initializes a Redis hash ring node.

        Args:
            conn: The Redis connection to use.
            key: A key to use for this node.
            n_replicas: Number of replicas this node should have on the ring.
            hash_algorithm: Hash algorithm to use. It is recommended to use
                `HashAlgorithm.XXHASH` (the default) because it provides better
                uniform distribution than CRC-32 with faster hashing. If you
                need to support hashrings created before we introduced support
                for xxHash, use `HashAlgorithm.CRC32`.
        """
        self._polling_thread = None
        self._stop_polling_fd_r = None
        self._stop_polling_fd_w = None

        self._conn = conn
        self._key = key

        if hash_algorithm is HashAlgorithm.XXHASH:
            if xxhash is None:
                raise ImportError(
                    "xxhash library is required for XXHASH algorithm. "
                    "Install with: pip install redis-hashring[xxhash]"
                )
            self._hash_function = _hash_with_xxhash
        elif hash_algorithm is HashAlgorithm.CRC32:
            self._hash_function = _hash_with_crc32
        else:
            raise ValueError("Unexpected hash algorithm requested")

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

        # Readers take no lock, and read `self._state` once per call. Writers
        # replace the whole state with one assignment, so a reader never sees
        # a partial state.
        self._state = _EMPTY_STATE
        # Makes sure that `update` and `remove` on different threads run one
        # at a time. The lock is reentrant: a signal handler that interrupts
        # `remove` can call `remove` again, and this lock does not block it.
        self._state_lock = threading.RLock()
        # Number of calls to `remove`. Changes only under `self._state_lock`.
        self._removal_count = 0

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

        To leave the ring while the node runs, use `stop()`.
        """
        # Hold the lock, so that an `update` that fetched the ring before the
        # ZREM cannot publish its ranges after the empty state.
        with self._state_lock:
            self._removal_count += 1
            # Own no keys from now on, even if the ZREM fails.
            self._state = _EMPTY_STATE

            try:
                pipeline = self._conn.pipeline()

                for replica in self._replicas:
                    pipeline.zrem(self._key, f"{replica[0]}:{replica[1]}")
                pipeline.execute()
            finally:
                # A signal handler on the main thread can call `update` during
                # the ZREM, and publish the ranges of a ring that still has
                # this node. Then clear the state again.
                self._state = _EMPTY_STATE

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
        # Hold the lock during the fetch too. Otherwise, a call that fetched
        # an older ring can finish last, and the older ring wins.
        with self._state_lock:
            removal_count = self._removal_count
            state = self._compute_state(self._fetch_ring())
            # If this call runs on the main thread, a signal handler can call
            # `remove` while this call holds the lock. Then the fetched ring
            # can be older than the removal, so do not publish it.
            if self._removal_count == removal_count:
                self._state = state
                # The signal handler can also run between the check and the
                # assignment. Then clear the state again.
                if self._removal_count != removal_count:
                    self._state = _EMPTY_STATE

    def _compute_state(self, ring):
        """
        Return the state of this node in the given ring.
        """
        nodes = set()
        n_replicas = len(ring)

        own_replicas = {r[1] for r in self._replicas}

        ranges = []
        for n, (start, replica) in enumerate(ring):
            host, pid, _ = replica.split(":")
            node = ":".join([host, pid])
            nodes.add(node)

            if replica in own_replicas:
                end = ring[(n + 1) % n_replicas][0] % RING_SIZE
                if start < end:
                    ranges.append((start, end))
                elif end < start:
                    ranges.append((start, RING_SIZE))
                    ranges.append((0, end))
                else:
                    ranges.append((0, RING_SIZE))

        return _RingState(ranges=tuple(ranges), node_count=len(nodes))

    def get_ranges(self):
        """
        Return a tuple of the hash ring ranges that this node owns.

        Each range is a tuple (a, b) that includes any ring point N with
        a <= N < b.
        """
        return self._state.ranges

    def get_node_count(self):
        """
        Return the number of active nodes in the ring.
        """
        return self._state.node_count

    def contains(self, key):
        """
        Check whether this node is responsible for the item.
        """
        return self._contains_ring_point(self.key_as_ring_point(key))

    def key_as_ring_point(self, key):
        """Turn a key into a point on a hash ring."""
        return self._hash_function(key)

    def _contains_ring_point(self, n):
        """
        Check whether this node is responsible for the ring point.
        """
        for start, end in self._state.ranges:
            if start <= n < end:
                return True
        return False

    def poll(self):
        """
        Keep a node in the hash ring.

        This should be kept running for as long as the node needs to stay in
        the ring. This takes care of:
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
                r, _, _ = select.select(
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
        Start the node.
        """
        self._polling_thread = threading.Thread(target=self.poll, daemon=True)
        self._polling_thread.start()

    def stop(self):
        """
        Stop the node.
        """
        if self._polling_thread:
            while not self._stop_polling_fd_w:
                # Let's give the thread some time to create the fd.
                time.sleep(0.1)
            os.write(self._stop_polling_fd_w, b"1")
            self._polling_thread.join()
            self._polling_thread = None
        self.remove()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args, **kwargs):
        self.stop()
