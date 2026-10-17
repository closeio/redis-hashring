import binascii
import collections
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

# Amount of points on the ring. Do not change this value. All nodes of a
# ring, in all releases, must agree on the point of each key. Applications
# can also store these points. The README and the docstrings state this value.
RING_SIZE = 2**32

# Default amount of replicas per node.
RING_REPLICAS = 16


class HashAlgorithm(enum.Enum):
    """
    The hash function that gives the point of a key on the ring.

    All nodes of a ring must use the same algorithm.
    """

    CRC32 = "crc32"
    XXHASH = "xxhash"

    def ring_point(self, key):
        """
        Return the point of `key` on the ring.

        `key` must be a `str`. The point is an integer N with 0 <= N < 2**32.
        Compare it with the ranges from `RingNode.get_ranges`.

        For `XXHASH`, raise `ImportError` if the `xxhash` library is not
        installed.
        """
        data = key.encode()
        if self is HashAlgorithm.XXHASH:
            _check_xxhash()
            return xxhash.xxh32_intdigest(data) % RING_SIZE
        if self is HashAlgorithm.CRC32:
            return binascii.crc32(data) % RING_SIZE
        raise ValueError(f"No hash function for {self!r}")


# The README and the docstrings of `RingNode` state some of the values below
# in seconds. For example, they state "50 seconds" for
# `NODE_TIMEOUT - POLL_INTERVAL`.
#
# If you change a value, change the text too.

# How often to update a node's heartbeat.
POLL_INTERVAL = 10

# After how much time a node is considered to be dead.
NODE_TIMEOUT = 60

# How often expired nodes are cleaned up from the ring.
CLEANUP_INTERVAL = 120

# How long a node waits for Redis to confirm its subscription to the ring.
SUBSCRIBE_TIMEOUT = 10

# How often the thread checks whether `stop` asked it to exit.
STOP_CHECK_INTERVAL = 1

# The retry interval after the first error.
RETRY_INTERVAL_MIN = 1

# The longest retry interval. It equals `POLL_INTERVAL`, so that a retry
# waits no longer than the interval between two heartbeats.
RETRY_INTERVAL_MAX = POLL_INTERVAL

# The state of a node:
# * ranges: tuple of the ranges that the node owns, where a range (a, b)
#   includes any N matching a <= N < b.
# * node_count: number of nodes currently active in the ring.
_RingState = collections.namedtuple("_RingState", ["ranges", "node_count"])

_EMPTY_STATE = _RingState(ranges=(), node_count=0)

# The README names this logger, so that applications can configure it.
_logger = logging.getLogger("redis_hashring")


def _check_xxhash():
    """
    Raise `ImportError` if the `xxhash` library is not installed.
    """
    if xxhash is None:
        raise ImportError(
            "xxhash library is required for XXHASH algorithm. "
            "Install with: pip install redis-hashring[xxhash]"
        )


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


def _log(level, message, *args, exc_info=None):
    """
    Log a message with `_logger`, and ignore errors from the log handlers.

    If `exc_info` is true, add the error that the current thread handles to
    the record.
    """
    try:
        _logger.log(level, message, *args, exc_info=exc_info)
    except Exception:
        # An error in a log handler must not end the thread.
        pass


def _deadline_after(last_heartbeat):
    """
    Return the time `NODE_TIMEOUT - POLL_INTERVAL` seconds after
    `last_heartbeat`.

    `last_heartbeat` and the result are `time.monotonic` values.
    """
    return last_heartbeat + NODE_TIMEOUT - POLL_INTERVAL


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
        # Only process items this node is responsible for. `item` must be a
        # `str`.
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
            # Only process items this node is responsible for. `item` must be
            # a `str`.
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
            key: The Redis key of the ring. All nodes of the ring use the same
                key.
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

        self._conn = conn
        self._key = key

        if not isinstance(hash_algorithm, HashAlgorithm):
            raise ValueError("Unexpected hash algorithm requested")
        if hash_algorithm is HashAlgorithm.XXHASH:
            # Raise now, not at the first call to `contains`.
            _check_xxhash()
        self._hash_algorithm = hash_algorithm

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
        # Makes sure that `_update` and `_remove` run one at a time.
        self._state_lock = threading.Lock()
        # The deadline: the `time.monotonic` value after which readers see an
        # empty state. Readers compare it with the time at each call, so no
        # Redis call can delay the deadline.
        self._deadline = math.inf

    def _fetch_ring(self):
        """
        Fetch the ring from Redis.

        The fetched ring only includes active nodes. Returns a list of tuples
        (start, replica), sorted by start, where start is the start of the
        range for which the replica is responsible.
        """
        expiry_time = time.time() - NODE_TIMEOUT
        data = self._conn.zrangebyscore(self._key, expiry_time, "INF")

        ring = []
        for replica_data in data:
            start, replica = _decode(replica_data).split(":", 1)
            ring.append((int(start), replica))
        return sorted(ring, key=operator.itemgetter(0))

    def _heartbeat(self):
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

    def _remove(self):
        """
        Remove the node from the ring.
        """
        # Hold the lock, so that an `_update` that fetched the ring before the
        # ZREM cannot publish its ranges after the empty state.
        with self._state_lock:
            # Own no keys from now on, even if the ZREM fails.
            self._state = _EMPTY_STATE

            pipeline = self._conn.pipeline()

            for replica in self._replicas:
                pipeline.zrem(self._key, f"{replica[0]}:{replica[1]}")
            pipeline.execute()

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
        arrives in `SUBSCRIBE_TIMEOUT` seconds, raise
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
            if pubsub.get_message(timeout=SUBSCRIBE_TIMEOUT) is None:
                raise RedisTimeoutError(
                    f"No reply to the subscription to {self._key!r}"
                    f" after {SUBSCRIBE_TIMEOUT} s."
                )
        except BaseException:
            _close_pubsub(pubsub)
            raise
        return pubsub

    def _cleanup(self):
        """
        Removes expired nodes from the ring.
        """
        expired = time.time() - NODE_TIMEOUT

        if self._conn.zremrangebyscore(self._key, 0, expired):
            self._notify()

    def _update(self):
        """
        Fetches the updated ring from Redis and updates the current ranges.
        """
        # Hold the lock during the fetch too. Otherwise, a call that fetched
        # an older ring can finish last, and the older ring wins.
        with self._state_lock:
            self._state = self._compute_state(self._fetch_ring())

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
        """
        Return `self._state`, or an empty state after `self._deadline`.
        """
        if time.monotonic() >= self._deadline:
            return _EMPTY_STATE
        return self._state

    @property
    def hash_algorithm(self):
        """
        The `HashAlgorithm` of this node.

        Compare the points from its `ring_point` with the ranges from
        `get_ranges`.
        """
        return self._hash_algorithm

    def get_ranges(self):
        """
        Return a tuple of the hash ring ranges that this node owns.

        Each range is a tuple (a, b) that includes any ring point N with
        a <= N < b. Compare the ranges with the points from
        `hash_algorithm.ring_point`.

        While the node runs, return an empty tuple after 50 seconds without a
        successful heartbeat and a successful read of the ring after it.
        """
        return self._read_state().ranges

    def get_node_count(self):
        """
        Return the number of active nodes in the ring.

        While the node runs, return 0 after 50 seconds without a successful
        heartbeat and a successful read of the ring after it.
        """
        return self._read_state().node_count

    def contains(self, key):
        """
        Check whether this node is responsible for `key`.

        `key` must be a `str`.

        While the node runs, return `False` after 50 seconds without a
        successful heartbeat and a successful read of the ring after it.
        """
        return self._contains_ring_point(self._hash_algorithm.ring_point(key))

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

    def _check_deadline(self, gave_up_keys, exc_info=None):
        """
        Return whether `self._deadline` passed.

        The node gives up its keys when the deadline passes. Log when the
        node gives up its keys, and when it gets them back.

        Args:
            gave_up_keys: The result of the previous call.
            exc_info: Whether to add the error that the current thread handles
                to the record about the keys that the node gave up.
        """
        now = time.monotonic()
        deadline_passed = now >= self._deadline
        if deadline_passed and not gave_up_keys:
            # The record can come long after the deadline, for example after
            # a Redis call that blocked. State the delay.
            _log(
                logging.ERROR,
                "The node in the ring %r gave up its keys %.1f seconds ago,"
                " after %s seconds without a successful heartbeat and read"
                " of the ring.",
                self._key,
                now - self._deadline,
                NODE_TIMEOUT - POLL_INTERVAL,
                exc_info=exc_info,
            )
        elif gave_up_keys and not deadline_passed:
            _log(
                logging.INFO,
                "The node in the ring %r got its keys back.",
                self._key,
            )
        return deadline_passed

    def _poll(self):
        """
        Keep the node in the ring until `stop` asks the thread to exit.

        First, run the setup: subscribe to the ring's activity channel, and
        run `_heartbeat`, `_update`, and `_cleanup`. Set `self._setup_done`
        when the setup ends. If the setup fails, store the error in
        `self._setup_error`. If the setup started to write to the ring, try
        to remove the node from the ring. Then return.

        After the setup, run `_run_loop`.
        """
        pubsub = None
        may_be_in_ring = False
        try:
            # Subscribe before `_update`, so that no change of the ring after
            # `_update` gets lost.
            pubsub = self._subscribe()

            # From here on, the ring can hold replicas of this node, even if
            # a call raises.
            may_be_in_ring = True
            # Take the time before each heartbeat, not after it. Thus, a slow
            # heartbeat does not delay the deadline.
            last_heartbeat = time.monotonic()
            self._heartbeat()

            self._update()

            self._cleanup()
            last_cleanup = time.monotonic()

            # The setup worked. From now on, readers see the state only until
            # the deadline of the last heartbeat.
            self._deadline = _deadline_after(last_heartbeat)
        except BaseException as error:
            self._setup_error = error
            _close_pubsub(pubsub)
            if may_be_in_ring:
                try:
                    # Undo the setup steps that worked before the error.
                    self._remove()
                except Exception:
                    # Keep the setup error, not this one.
                    pass
            return
        finally:
            self._setup_done.set()

        self._run_loop(pubsub, last_heartbeat, last_cleanup)

    def _run_loop(self, pubsub, last_heartbeat, last_cleanup):
        """
        Keep the node in the ring until `stop` asks the thread to exit.

        The loop does these tasks:
        * Update the heartbeat.
        * Update the ranges when the ring changes, and after each heartbeat.
        * Remove expired nodes from the ring periodically.
        * After each heartbeat and the update after it, set the deadline to
          `NODE_TIMEOUT - POLL_INTERVAL` seconds after the start of the
          heartbeat. If a heartbeat ends after the deadline of the previous
          heartbeat, notify the other nodes.

        Log each error, and try again, until `stop` asks the thread to exit.
        Then close the pub/sub object, clear the state, and remove the
        deadline.

        Log when the node gives up its keys, and when it gets them back.
        Check after each failed attempt and the wait after it, and after each
        heartbeat and the update after it.

        Args:
            pubsub: A pub/sub object, subscribed to the ring's activity
                channel.
            last_heartbeat: The `time.monotonic` value at the start of the
                last heartbeat.
            last_cleanup: The `time.monotonic` value at the end of the last
                cleanup.
        """
        retry_interval = RETRY_INTERVAL_MIN
        # Whether the deadline passed at the last check.
        gave_up_keys = False
        try:
            while not self._stop_requested.is_set():
                try:
                    resubscribed = pubsub is None
                    if resubscribed:
                        pubsub = self._subscribe()
                        # If `stop` asked the thread to exit during the
                        # subscription, exit before more Redis calls.
                        if self._stop_requested.is_set():
                            break

                    now = time.monotonic()
                    if resubscribed or now - last_heartbeat >= POLL_INTERVAL:
                        heartbeat_started = now
                        previous_deadline = _deadline_after(last_heartbeat)
                        self._heartbeat()
                        # If `stop` asked the thread to exit during the
                        # heartbeat, exit before more Redis calls. The node
                        # leaves the ring, so the notice and the update are
                        # not necessary.
                        if self._stop_requested.is_set():
                            break
                        if time.monotonic() >= previous_deadline:
                            # The heartbeat ended after the deadline, so the
                            # other nodes can see the replicas of this node
                            # as expired. Make them read the ring again, so
                            # that they give back the keys of this node. If
                            # the notice fails, `last_heartbeat` keeps its
                            # value, and the next attempt sends it again.
                            self._notify()
                        last_heartbeat = heartbeat_started
                        # Read the ring after each heartbeat. Thus, the node
                        # sees the changes of the ring that it missed. A new
                        # subscription, or a pub/sub connection that died
                        # without an error, can miss changes.
                        self._update()
                        # A Redis call that blocks can let the deadline pass
                        # with no error. Check before the deadline moves.
                        gave_up_keys = self._check_deadline(gave_up_keys)
                        # Move the deadline after `_update`, not after the
                        # heartbeat. Thus, if `_update` fails after the
                        # deadline, readers do not see the old ranges again.
                        self._deadline = _deadline_after(last_heartbeat)
                        gave_up_keys = self._check_deadline(gave_up_keys)

                    # If `stop` asked the thread to exit during the update,
                    # exit before the wait for a notice.
                    if self._stop_requested.is_set():
                        break

                    # Wait for a notice until the next heartbeat is due. Wake
                    # up every `STOP_CHECK_INTERVAL` seconds to check for a
                    # request to stop.
                    next_heartbeat_at = last_heartbeat + POLL_INTERVAL
                    timeout = min(
                        STOP_CHECK_INTERVAL,
                        max(0.0, next_heartbeat_at - time.monotonic()),
                    )
                    if pubsub.get_message(timeout=timeout) is not None:
                        while pubsub.get_message() is not None:
                            pass
                        self._update()

                    if time.monotonic() - last_cleanup >= CLEANUP_INTERVAL:
                        self._cleanup()
                        last_cleanup = time.monotonic()

                    retry_interval = RETRY_INTERVAL_MIN
                except Exception:
                    _log(
                        logging.WARNING,
                        "An attempt of the node in the ring %r failed. The"
                        " node will try again.",
                        self._key,
                        exc_info=True,
                    )
                    gave_up_keys = self._check_deadline(
                        gave_up_keys, exc_info=True
                    )
                    _close_pubsub(pubsub)
                    pubsub = None
                    self._wait_before_retry(retry_interval)
                    # The deadline can pass during the wait. Check again
                    # while the thread still handles the error, so that the
                    # record holds it.
                    gave_up_keys = self._check_deadline(
                        gave_up_keys, exc_info=True
                    )
                    retry_interval = min(
                        retry_interval * 2, RETRY_INTERVAL_MAX
                    )
        finally:
            _close_pubsub(pubsub)
            # No heartbeat keeps the node in the ring from now on. Clear the
            # state before the deadline goes away, so that readers do not see
            # old ranges.
            self._clear_state()
            self._deadline = math.inf

    def start(self):
        """
        Start the node.

        Return when the node is in the ring and has its ranges. If the node
        cannot join the ring, raise the error of the thread. Before that, try
        to remove the replicas that the node added to the ring, if any.

        Raise `RuntimeError` if the node already started.

        Do not call `start` or `stop` from two threads at the same time.
        """
        if self._polling_thread is not None:
            raise RuntimeError("The node already started.")

        self._setup_done.clear()
        self._setup_error = None
        self._stop_requested.clear()
        polling_thread = threading.Thread(
            target=self._poll, name=f"redis-hashring-{self._key}", daemon=True
        )
        polling_thread.start()
        # Set the attribute only after the thread starts. If `Thread.start`
        # raises, the node stays as if it never started.
        self._polling_thread = polling_thread
        self._setup_done.wait()
        if self._setup_error is not None:
            setup_error = self._setup_error
            # Drop the reference. The traceback of the error refers to `self`.
            self._setup_error = None
            self._polling_thread.join()
            self._polling_thread = None
            raise setup_error

    def stop(self):
        """
        Stop the node, and remove it from the ring.

        This takes up to 1 second, plus the time of the Redis calls in
        progress.

        Do not call `start` or `stop` from two threads at the same time.
        """
        if self._polling_thread is not None:
            self._stop_requested.set()
            self._polling_thread.join()
            self._polling_thread = None
            # A `start` call that did not return, for example after a
            # `KeyboardInterrupt`, can leave the setup in progress. If that
            # setup fails, drop the error. Its traceback refers to `self`.
            self._setup_error = None
        self._remove()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args, **kwargs):
        self.stop()
