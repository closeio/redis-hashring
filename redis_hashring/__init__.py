import binascii
import collections
import contextlib
import enum
import logging
import math
import operator
import os
import random
import socket
import threading
import time

from redis.exceptions import TimeoutError as RedisTimeoutError

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
POLL_INTERVAL_SECONDS = 10

# After how much time a node is considered to be dead.
NODE_TIMEOUT_SECONDS = 60

# How often expired nodes are cleaned up from the ring.
CLEANUP_INTERVAL_SECONDS = 120

# How long a node waits for Redis to confirm its subscription to the ring.
SUBSCRIBE_TIMEOUT_SECONDS = 10

# How often the thread checks whether `stop` asked it to exit.
STOP_CHECK_INTERVAL_SECONDS = 1

# The retry interval after the first error.
RETRY_INTERVAL_MIN_SECONDS = 1

# The longest retry interval. It equals `POLL_INTERVAL_SECONDS`, so that a
# retry waits no longer than the interval between two heartbeats.
RETRY_INTERVAL_MAX_SECONDS = POLL_INTERVAL_SECONDS

# The state of a node:
# * ranges: tuple of the ranges that the node owns, where a range (a, b)
#   includes any N matching a <= N < b.
# * node_count: number of nodes currently active in the ring.
_RingState = collections.namedtuple("_RingState", ["ranges", "node_count"])

_EMPTY_STATE = _RingState(ranges=(), node_count=0)

# Applications can configure this logger without changing thread error hooks.
_logger = logging.getLogger("redis_hashring")


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


def _close_pubsub(pubsub):
    """
    Close the pub/sub object, if any, and ignore errors from the close.
    """
    if pubsub is not None:
        try:
            pubsub.close()
        except Exception:
            pass


def _deadline_after(last_heartbeat):
    """
    Return the time `NODE_TIMEOUT_SECONDS - POLL_INTERVAL_SECONDS` seconds
    after `last_heartbeat`.

    `last_heartbeat` and the result are `time.monotonic` values.
    """
    return last_heartbeat + NODE_TIMEOUT_SECONDS - POLL_INTERVAL_SECONDS


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
        # Set when `stop` asks the thread to exit.
        self._stop_requested = threading.Event()
        # Set when the setup of the thread ends, with or without an error.
        self._setup_done = threading.Event()
        # The error that ended the setup, until `start` raises it.
        self._setup_error = None
        # Held by the `start` or `stop` call in progress, if any.
        self._lifecycle_lock = threading.Lock()

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

        # Readers take no lock, and read the state once per call, through
        # `_read_state`. Writers replace the whole state with one assignment,
        # so a reader never sees a partial state.
        self._state = _EMPTY_STATE
        # Makes sure that `update` and `remove` on different threads run one
        # at a time. The lock is reentrant: a signal handler that interrupts
        # `remove` can call `remove` again, and this lock does not block it.
        self._state_lock = threading.RLock()
        # Number of calls to `remove`. Changes only under `self._state_lock`.
        self._removal_count = 0
        # Heartbeats can succeed while ring reads fail. Only a complete
        # renewal extends ownership validity. The polling thread writes both
        # timestamps; readers only check `_ownership_valid_until`.
        self._last_heartbeat_at = 0.0
        # The deadline: the `time.monotonic` value after which readers see an
        # empty state. Readers compare it with the time at each call, so no
        # Redis call can delay the deadline.
        self._ownership_valid_until = math.inf
        # Logging state, observed only by the polling thread.
        self._reported_ownership_expired = False

    def _fetch_ring(self):
        """
        Fetch the ring from Redis.

        The fetched ring only includes active nodes. Returns a list of tuples
        (start, replica) (see _fetch_all docs for more details).
        """
        expiry_time = time.time() - NODE_TIMEOUT_SECONDS
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
        expiry_time = time.time() - NODE_TIMEOUT_SECONDS
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

    def _subscribe(self):
        """
        Return a new pub/sub object, subscribed to the ring's activity channel.

        Return after Redis confirms the subscription. After that, Redis sends
        each notice on the channel to the pub/sub object. If no confirmation
        arrives in `SUBSCRIBE_TIMEOUT_SECONDS` seconds, raise
        `redis.exceptions.TimeoutError`.

        If a step fails, close the pub/sub object before the error goes up.
        """
        # Keep subscribe messages, so that `get_message` returns the reply to
        # the subscription.
        pubsub = self._conn.pubsub(ignore_subscribe_messages=False)
        try:
            pubsub.subscribe(self._key)
            # Wait with a limit of our own, so that the call ends even if
            # Redis does not reply.
            if pubsub.get_message(timeout=SUBSCRIBE_TIMEOUT_SECONDS) is None:
                raise RedisTimeoutError(
                    f"No reply to the subscription to {self._key!r}"
                    f" after {SUBSCRIBE_TIMEOUT_SECONDS} s."
                )
        except BaseException:
            _close_pubsub(pubsub)
            raise
        return pubsub

    def cleanup(self):
        """
        Removes expired nodes from the ring.
        """
        expired = time.time() - NODE_TIMEOUT_SECONDS

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

    def _clear_state(self):
        """
        Make sure that this node owns no keys.
        """
        with self._state_lock:
            self._state = _EMPTY_STATE

    def _read_state(self):
        """Return ownership only while its renewal is still valid."""
        if time.monotonic() >= self._ownership_valid_until:
            return _EMPTY_STATE
        return self._state

    def get_ranges(self):
        """
        Return a tuple of the hash ring ranges that this node owns.

        Each range is a tuple (a, b) that includes any ring point N with
        a <= N < b.

        While the node runs, return an empty tuple after
        `NODE_TIMEOUT_SECONDS - POLL_INTERVAL_SECONDS` seconds without a
        heartbeat and a successful `update` after it.
        """
        return self._read_state().ranges

    def get_node_count(self):
        """
        Return the number of active nodes in the ring.

        While the node runs, return 0 after
        `NODE_TIMEOUT_SECONDS - POLL_INTERVAL_SECONDS` seconds without a
        heartbeat and a successful `update` after it.
        """
        return self._read_state().node_count

    def contains(self, key):
        """
        Check whether this node is responsible for the item.

        While the node runs, return `False` after
        `NODE_TIMEOUT_SECONDS - POLL_INTERVAL_SECONDS` seconds without a
        heartbeat and a successful `update` after it.
        """
        return self._contains_ring_point(self.key_as_ring_point(key))

    def key_as_ring_point(self, key):
        """Turn a key into a point on a hash ring."""
        return self._hash_function(key)

    def _contains_ring_point(self, n):
        """
        Check whether this node is responsible for the ring point.
        """
        for start, end in self._read_state().ranges:
            if start <= n < end:
                return True
        return False

    def _wait_before_retry(self, retry_interval):
        """
        Wait before the next attempt after an error.

        Wait for a random time between half `retry_interval` and the full
        interval, so that the nodes do not all try again at the same time.
        Return early if `self._stop_requested` is set.
        """
        self._stop_requested.wait(retry_interval * random.uniform(0.5, 1))

    def _log_ownership_change(self, *, exc_info=None):
        """Report each expiry and recovery once, when the thread observes it."""
        now = time.monotonic()
        expired = now >= self._ownership_valid_until
        if expired == self._reported_ownership_expired:
            return
        self._reported_ownership_expired = expired

        # Application log handlers must not interrupt membership maintenance.
        with contextlib.suppress(Exception):
            if expired:
                _logger.error(
                    "The node in the ring %r gave up its keys %.1f seconds"
                    " ago, after %s seconds without a successful heartbeat"
                    " and read of the ring.",
                    self._key,
                    now - self._ownership_valid_until,
                    NODE_TIMEOUT_SECONDS - POLL_INTERVAL_SECONDS,
                    exc_info=exc_info,
                )
            else:
                _logger.info(
                    "The node in the ring %r got its keys back.", self._key
                )

    def _poll(self):
        """
        Keep the node in the ring until `stop` asks the thread to exit.

        First, run the setup: subscribe to the ring's activity channel, and
        run `heartbeat`, `update`, and `cleanup`. Set `self._setup_done` when
        the setup ends. If the setup fails, store the error in
        `self._setup_error`. If the setup started to write to the ring, try
        to remove the node from the ring. Then return.

        After the setup, run `_run_loop`.
        """
        pubsub = None
        may_be_in_ring = False
        try:
            # Subscribe before `update`, so that no change of the ring after
            # `update` gets lost.
            pubsub = self._subscribe()

            # From here on, the ring can hold replicas of this node, even if
            # a call raises.
            may_be_in_ring = True
            # Take the time before each heartbeat, not after it. Thus, a slow
            # heartbeat does not delay the deadline.
            self._last_heartbeat_at = time.monotonic()
            self.heartbeat()

            self.update()

            last_cleanup_at = time.monotonic()
            self.cleanup()

            # The setup worked. From now on, readers see the state only until
            # the deadline of the last heartbeat.
            self._ownership_valid_until = _deadline_after(
                self._last_heartbeat_at
            )
            self._reported_ownership_expired = False
        except BaseException as error:
            self._setup_error = error
            _close_pubsub(pubsub)
            if may_be_in_ring:
                try:
                    # Undo the setup steps that worked before the error.
                    self.remove()
                except Exception:
                    # Keep the setup error, not this one.
                    pass
            return
        finally:
            self._setup_done.set()

        self._run_loop(pubsub, last_cleanup_at)

    def _renew_ownership(self, heartbeat_started_at):
        """
        Renew membership and ownership, in that order.

        A failed notice leaves the heartbeat timestamp unchanged, so the
        next attempt sends the notice again. A failed ring read keeps the
        new heartbeat timestamp but does not extend ownership validity.
        """
        previous_deadline = _deadline_after(self._last_heartbeat_at)
        self.heartbeat()
        if self._stop_requested.is_set():
            return

        # Peers may have taken over after the previous heartbeat expired.
        if time.monotonic() >= previous_deadline:
            self._notify()
        self._last_heartbeat_at = heartbeat_started_at

        # Refresh even without a notice: a lost pub/sub connection can miss
        # changes. Publish the new ranges before extending their validity.
        self.update()
        # A blocking Redis call may have let ownership expire without an
        # exception. Observe that transition before advancing the deadline.
        self._log_ownership_change()
        self._ownership_valid_until = _deadline_after(heartbeat_started_at)
        self._log_ownership_change()

    def _wait_for_ring_change(self, pubsub):
        """Wait for notices, bounded by the next heartbeat and stop check."""
        next_heartbeat_at = self._last_heartbeat_at + POLL_INTERVAL_SECONDS
        timeout = min(
            STOP_CHECK_INTERVAL_SECONDS,
            max(0.0, next_heartbeat_at - time.monotonic()),
        )
        if pubsub.get_message(timeout=timeout) is not None:
            while pubsub.get_message() is not None:
                pass
            self.update()

    def _run_loop(self, pubsub, last_cleanup_at):
        """Maintain the ring, retrying failed attempts until stopped."""
        retry_interval = RETRY_INTERVAL_MIN_SECONDS
        try:
            while not self._stop_requested.is_set():
                try:
                    resubscribed = pubsub is None
                    if resubscribed:
                        pubsub = self._subscribe()
                        if self._stop_requested.is_set():
                            break

                    now = time.monotonic()
                    if (
                        resubscribed
                        or now - self._last_heartbeat_at
                        >= POLL_INTERVAL_SECONDS
                    ):
                        self._renew_ownership(now)
                    if self._stop_requested.is_set():
                        break

                    self._wait_for_ring_change(pubsub)
                    now = time.monotonic()
                    if now - last_cleanup_at >= CLEANUP_INTERVAL_SECONDS:
                        self.cleanup()
                        last_cleanup_at = now

                    retry_interval = RETRY_INTERVAL_MIN_SECONDS
                except Exception:
                    with contextlib.suppress(Exception):
                        _logger.warning(
                            "An attempt of the node in the ring %r failed."
                            " The node will try again.",
                            self._key,
                            exc_info=True,
                        )
                    self._log_ownership_change(exc_info=True)
                    _close_pubsub(pubsub)
                    pubsub = None
                    self._wait_before_retry(retry_interval)
                    self._log_ownership_change(exc_info=True)
                    retry_interval = min(
                        retry_interval * 2, RETRY_INTERVAL_MAX_SECONDS
                    )
        finally:
            _close_pubsub(pubsub)
            # Clear ownership before removing the deadline, so readers cannot
            # see stale ranges after the polling thread exits.
            self._clear_state()
            self._ownership_valid_until = math.inf

    @contextlib.contextmanager
    def _hold_lifecycle_lock(self):
        """
        Hold the lifecycle lock during the block.

        Raise `RuntimeError` if another `start` or `stop` call holds the lock.
        Do not wait for the lock. If a signal handler calls `stop` during
        `start` on the same thread, a wait never ends, because the thread
        waits for itself.
        """
        if not self._lifecycle_lock.acquire(blocking=False):
            raise RuntimeError(
                "Another `start` or `stop` call is in progress."
            )
        try:
            yield
        finally:
            self._lifecycle_lock.release()

    def start(self):
        """
        Start the node.

        Return when the node is in the ring and has its ranges. If the node
        cannot join the ring, raise the error of the thread. Before that, try
        to remove the replicas that the node added to the ring, if any.

        Raise `RuntimeError` if the node already started, or if another
        `start` or `stop` call on the node is in progress.
        """
        with self._hold_lifecycle_lock():
            if self._polling_thread is not None:
                raise RuntimeError("The node already started.")

            self._setup_done.clear()
            self._setup_error = None
            self._stop_requested.clear()
            polling_thread = threading.Thread(
                target=self._poll,
                name=f"redis-hashring-{self._key}",
                daemon=True,
            )
            polling_thread.start()
            # Set the attribute only after the thread starts. If
            # `Thread.start` raises, the node stays as if it never started.
            self._polling_thread = polling_thread
            self._setup_done.wait()
            if self._setup_error is not None:
                setup_error = self._setup_error
                # Drop the reference. The traceback of the error refers to
                # `self`.
                self._setup_error = None
                self._polling_thread.join()
                self._polling_thread = None
                raise setup_error

    def stop(self):
        """
        Stop the node, and remove it from the ring.

        This takes up to `STOP_CHECK_INTERVAL_SECONDS` seconds, plus the time
        of the Redis calls in progress.

        Raise `RuntimeError` if another `start` or `stop` call on the node is
        in progress.
        """
        with self._hold_lifecycle_lock():
            if self._polling_thread is not None:
                self._stop_requested.set()
                self._polling_thread.join()
                self._polling_thread = None
                # If a `start` call did not return and its setup failed, the
                # error stays in `self._setup_error`, because no call raised
                # it. Clear the attribute. The traceback of the error holds
                # the frames of the thread, and these frames refer to `self`.
                # This reference cycle keeps the node in memory until the
                # garbage collector runs.
                self._setup_error = None
            self.remove()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args, **kwargs):
        self.stop()
