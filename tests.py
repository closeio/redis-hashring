import itertools
import math
import socket
import threading
import time
from unittest.mock import Mock, patch

import pytest
from redis import Redis
from redis.client import PubSub
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    TimeoutError as RedisTimeoutError,
)

import redis_hashring
from redis_hashring import (
    RING_SIZE,
    SUBSCRIBE_TIMEOUT,
    HashAlgorithm,
    RingNode,
)

TEST_KEY = "hashring-test"

# The name of the Redis connections of the tests.
TEST_CLIENT_NAME = "hashring-test"

OTHER_REPLICA = "host2:1:00000000"


@pytest.fixture
def redis():
    redis = Redis(client_name=TEST_CLIENT_NAME)
    yield redis
    redis.delete(TEST_KEY)


@pytest.fixture(autouse=True)
def short_stop_check_interval():
    """
    Make the thread check for a request to stop every 0.05 s.
    """
    with patch.object(redis_hashring, "STOP_CHECK_INTERVAL", 0.05):
        yield


def get_node(redis, n_replicas, total_replicas, hash_algorithm):
    node = RingNode(
        redis, TEST_KEY, n_replicas=n_replicas, hash_algorithm=hash_algorithm
    )

    assert len(node._replicas) == n_replicas
    assert redis.zcard(TEST_KEY) == total_replicas - n_replicas

    node.heartbeat()

    assert redis.zcard(TEST_KEY) == total_replicas
    assert len(node.get_ranges()) == 0

    return node


def test_node(redis):
    with patch.object(socket, "gethostname", return_value="host1"):
        node1 = get_node(redis, 1, 1, HashAlgorithm.XXHASH)
    node1.update()
    assert len(node1.get_ranges()) == 1
    assert node1.get_node_count() == 1

    with patch.object(socket, "gethostname", return_value="host2"):
        node2 = get_node(redis, 1, 2, HashAlgorithm.XXHASH)
    node1.update()
    node2.update()
    assert len(node1.get_ranges()) + len(node2.get_ranges()) == 3
    assert node1.get_node_count() == 2
    assert node2.get_node_count() == 2

    with patch.object(socket, "gethostname", return_value="host3"):
        node3 = get_node(redis, 2, 4, HashAlgorithm.XXHASH)
    node1.update()
    node2.update()
    node3.update()
    assert (
        len(node1.get_ranges())
        + len(node2.get_ranges())
        + len(node3.get_ranges())
        == 5
    )
    assert node1.get_node_count() == 3
    assert node2.get_node_count() == 3
    assert node3.get_node_count() == 3

    node1.remove()
    node2.update()
    node3.update()
    assert len(node1.get_ranges()) == 0
    assert node1.get_node_count() == 0
    assert len(node2.get_ranges()) + len(node3.get_ranges()) == 4
    assert node2.get_node_count() == 2
    assert node3.get_node_count() == 2


@pytest.mark.parametrize(
    "hash_algorithm", [HashAlgorithm.CRC32, HashAlgorithm.XXHASH]
)
def test_contains(redis, hash_algorithm):
    node1 = get_node(redis, 1, 1, hash_algorithm=hash_algorithm)
    node1.update()
    assert node1.contains("item") is True

    node1.remove()
    assert node1.contains("item") is False


def find_key(node, start, end):
    """
    Return a key whose ring point is in the range [start, end).
    """
    for n in itertools.count():
        key = str(n)
        if start <= node.key_as_ring_point(key) < end:
            return key


def start_thread(target):
    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    return thread


def join_thread(thread):
    thread.join(timeout=5)
    assert not thread.is_alive()


class PausingRing(list):
    """
    A ring that pauses after the first item of an iteration.

    The iteration sets `paused`, and waits for `resume` before it goes on.
    """

    def __init__(self, ring):
        super().__init__(ring)
        self.paused = threading.Event()
        self.resume = threading.Event()

    def __iter__(self):
        iterator = super().__iter__()
        yield next(iterator)
        self.paused.set()
        self.resume.wait()
        yield from iterator


class BlockingFetch:
    """
    A stand-in for `RingNode._fetch_ring`.

    Each call returns the next ring. The first call sets `started`, and waits
    for `release` before it returns.
    """

    def __init__(self, *rings):
        self.rings = rings
        self.call_count = 0
        self.started = threading.Event()
        self.release = threading.Event()

    def __call__(self):
        index = self.call_count
        self.call_count += 1
        if index == 0:
            self.started.set()
            self.release.wait()
        return self.rings[index]


def test_get_ranges_returns_tuple(redis):
    node = get_node(redis, 1, 1, HashAlgorithm.XXHASH)
    assert node.get_ranges() == ()

    node.update()
    assert isinstance(node.get_ranges(), tuple)
    assert node.get_ranges() == ((0, RING_SIZE),)

    node.remove()
    assert node.get_ranges() == ()


def test_readers_see_old_state_during_update(redis):
    node = RingNode(redis, TEST_KEY, n_replicas=1)
    own_replica = node._replicas[0][1]
    # A key that the node owns before the update, and not after it.
    key = find_key(node, 2**31, RING_SIZE)

    node._fetch_ring = Mock(return_value=[(0, own_replica)])
    node.update()

    ring = PausingRing([(0, own_replica), (2**31, OTHER_REPLICA)])
    node._fetch_ring = Mock(return_value=ring)
    update_thread = start_thread(node.update)
    assert ring.paused.wait(timeout=5)

    assert node.get_ranges() == ((0, RING_SIZE),)
    assert node.get_node_count() == 1
    assert node.contains(key) is True

    ring.resume.set()
    join_thread(update_thread)

    assert node.get_ranges() == ((0, 2**31),)
    assert node.get_node_count() == 2
    assert node.contains(key) is False


def test_update_calls_run_one_at_a_time(redis):
    node = RingNode(redis, TEST_KEY, n_replicas=1)
    own_replica = node._replicas[0][1]
    fetch_ring = BlockingFetch(
        [(0, own_replica)],
        [(0, own_replica), (2**31, OTHER_REPLICA)],
    )
    node._fetch_ring = fetch_ring

    first_update_thread = start_thread(node.update)
    assert fetch_ring.started.wait(timeout=5)
    second_update_thread = start_thread(node.update)
    # The second call waits for the first call, so it does not fetch yet.
    second_update_thread.join(timeout=0.1)
    assert second_update_thread.is_alive()
    assert fetch_ring.call_count == 1

    fetch_ring.release.set()
    join_thread(first_update_thread)
    join_thread(second_update_thread)

    # The state comes from the second fetch.
    assert fetch_ring.call_count == 2
    assert node.get_ranges() == ((0, 2**31),)
    assert node.get_node_count() == 2


def test_failed_update_keeps_old_state(redis):
    node = RingNode(redis, TEST_KEY, n_replicas=1)
    own_replica = node._replicas[0][1]
    node._fetch_ring = Mock(return_value=[(0, own_replica)])
    node.update()

    # The name of the second replica is malformed. The update computes the
    # first range, and then fails.
    node._fetch_ring = Mock(
        return_value=[(0, own_replica), (2**31, "malformed")]
    )
    with pytest.raises(ValueError):
        node.update()

    assert node.get_ranges() == ((0, RING_SIZE),)
    assert node.get_node_count() == 1
    assert not node._state_lock.locked()


def test_remove_waits_for_update(redis):
    node = get_node(redis, 1, 1, HashAlgorithm.XXHASH)
    own_replica = node._replicas[0][1]
    fetch_ring = BlockingFetch([(0, own_replica)])
    node._fetch_ring = fetch_ring

    update_thread = start_thread(node.update)
    assert fetch_ring.started.wait(timeout=5)
    remove_thread = start_thread(node.remove)
    # `remove` waits for `update`, so the node is still in the ring.
    remove_thread.join(timeout=0.1)
    assert remove_thread.is_alive()
    assert redis.zcard(TEST_KEY) == 1

    fetch_ring.release.set()
    join_thread(update_thread)
    join_thread(remove_thread)

    assert redis.zcard(TEST_KEY) == 0
    assert node.get_ranges() == ()
    assert node.get_node_count() == 0


def test_remove_clears_state_when_zrem_fails(redis):
    node = get_node(redis, 1, 1, HashAlgorithm.XXHASH)
    node.update()

    with patch(
        "redis.client.Pipeline.execute", side_effect=RedisConnectionError()
    ):
        with pytest.raises(RedisConnectionError):
            node.remove()

    assert node.get_ranges() == ()
    assert node.get_node_count() == 0
    assert not node._state_lock.locked()


def test_start_waits_for_ranges(redis):
    with RingNode(redis, TEST_KEY) as node:
        ranges = node.get_ranges()
        assert sum(end - start for start, end in ranges) == RING_SIZE
        assert node.get_node_count() == 1
        assert node.contains("item") is True


def test_start_reads_subscription_reply_before_update(redis):
    node = RingNode(redis, TEST_KEY)
    events = []
    original_get_message = PubSub.get_message
    original_update = node.update

    def get_message(pubsub, *args, **kwargs):
        message = original_get_message(pubsub, *args, **kwargs)
        if message is not None:
            events.append(message["type"])
        return message

    def update():
        events.append("update")
        original_update()

    with patch.object(
        PubSub, "get_message", autospec=True, side_effect=get_message
    ):
        with patch.object(node, "update", side_effect=update):
            with node:
                pass

    # The setup reads the reply to the subscription before it runs `update`.
    assert events[:2] == ["subscribe", "update"]


def test_start_raises_when_subscription_gets_no_reply(redis):
    node = RingNode(redis, TEST_KEY)
    threads_before = set(threading.enumerate())

    with patch.object(PubSub, "get_message", return_value=None) as get_message:
        with pytest.raises(RedisTimeoutError, match=TEST_KEY):
            node.start()

    # The setup waits for the reply with a limit.
    get_message.assert_called_once_with(timeout=SUBSCRIBE_TIMEOUT)
    assert set(threading.enumerate()) <= threads_before
    assert node._polling_thread is None
    # The setup failed before it wrote to the ring.
    assert redis.zcard(TEST_KEY) == 0


def test_start_twice_raises(redis):
    with RingNode(redis, TEST_KEY) as node:
        with pytest.raises(RuntimeError, match="already started"):
            node.start()

        assert node._polling_thread.is_alive()


def test_start_after_stop(redis):
    node = RingNode(redis, TEST_KEY)
    with node:
        first_polling_thread = node._polling_thread
    assert not first_polling_thread.is_alive()

    with node:
        assert node.get_node_count() == 1
        assert node.contains("item") is True


def test_start_raises_when_setup_fails(redis):
    node = RingNode(redis, TEST_KEY)
    threads_before = set(threading.enumerate())
    error = RedisConnectionError()
    original_close = PubSub.close

    with patch.object(
        PubSub, "close", autospec=True, side_effect=original_close
    ) as close:
        with patch.object(node, "heartbeat", side_effect=error):
            with pytest.raises(RedisConnectionError) as exc_info:
                node.start()

    assert exc_info.value is error
    # The setup closes the pub/sub object that it created.
    close.assert_called_once()
    assert set(threading.enumerate()) <= threads_before
    assert node._polling_thread is None
    assert node._setup_error is None
    # No thread runs, so `stop` does not wait.
    join_thread(start_thread(node.stop))


def test_start_after_failed_start(redis):
    node = RingNode(redis, TEST_KEY)
    with patch.object(node, "heartbeat", side_effect=RedisConnectionError()):
        with pytest.raises(RedisConnectionError):
            node.start()

    with node:
        assert node.get_node_count() == 1
        assert node.contains("item") is True


def test_start_raises_when_thread_cannot_start(redis):
    node = RingNode(redis, TEST_KEY)
    error = RuntimeError("can't start new thread")

    with patch.object(threading.Thread, "start", side_effect=error):
        with pytest.raises(RuntimeError) as exc_info:
            node.start()

    assert exc_info.value is error
    assert node._polling_thread is None
    # No thread runs, so `stop` does not wait.
    join_thread(start_thread(node.stop))

    with node:
        assert node.contains("item") is True


def test_failed_start_leaves_the_ring(redis):
    node = RingNode(redis, TEST_KEY)

    # The setup fails after the node joined the ring and got its ranges.
    with patch.object(node, "cleanup", side_effect=RedisConnectionError()):
        with pytest.raises(RedisConnectionError):
            node.start()

    assert node.get_ranges() == ()
    assert node.get_node_count() == 0
    assert redis.zcard(TEST_KEY) == 0


def test_failed_heartbeat_leaves_the_ring(redis):
    node = RingNode(redis, TEST_KEY)

    # `heartbeat` adds the replicas, and then fails to notify the other
    # nodes.
    with patch.object(node, "_notify", side_effect=RedisConnectionError()):
        with pytest.raises(RedisConnectionError):
            node.start()

    assert redis.zcard(TEST_KEY) == 0


def test_failed_start_before_heartbeat_does_not_remove(redis):
    node = RingNode(redis, TEST_KEY)

    with patch.object(PubSub, "subscribe", side_effect=RedisConnectionError()):
        with patch.object(node, "remove") as remove:
            with pytest.raises(RedisConnectionError):
                node.start()

    # The node never wrote to the ring, so it has nothing to remove.
    remove.assert_not_called()


def test_start_raises_setup_error_when_remove_fails(redis):
    node = RingNode(redis, TEST_KEY)
    error = RedisConnectionError()

    with patch.object(node, "heartbeat", side_effect=error):
        with patch.object(node, "remove", side_effect=RedisConnectionError()):
            with pytest.raises(RedisConnectionError) as exc_info:
                node.start()

    assert exc_info.value is error


def wait_for(condition, timeout=5):
    """
    Wait until `condition()` returns a true value. Fail after `timeout`
    seconds.
    """
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() >= deadline:
            pytest.fail(f"The condition is still false after {timeout} s.")
        time.sleep(0.01)


# The thread of a node passes each error to `threading.excepthook`. pytest
# turns these errors into warnings.
ignore_thread_errors = pytest.mark.filterwarnings(
    "ignore::pytest.PytestUnhandledThreadExceptionWarning"
)


class FailingCall:
    """
    A stand-in for a method of `RingNode`.

    While `failures_left` is above zero, each call decrements it, and raises
    `redis.exceptions.ConnectionError`. After that, each call runs the real
    method.
    """

    def __init__(self, method, failures_left):
        self.method = method
        self.failures_left = failures_left
        self.failure_count = 0
        self.success_count = 0
        # The thread of the node decrements `failures_left`, and the test
        # calls `heal`. The lock makes sure that no write gets lost.
        self.lock = threading.Lock()

    def __call__(self):
        with self.lock:
            fails = self.failures_left > 0
            if fails:
                self.failures_left -= 1
        if fails:
            self.failure_count += 1
            raise RedisConnectionError()
        result = self.method()
        self.success_count += 1
        return result

    def heal(self):
        """
        Make each later call run the real method.

        A call that already started to fail still fails.
        """
        with self.lock:
            self.failures_left = 0


class BlockingCall:
    """
    A stand-in for a method of `RingNode`.

    The first call sets `started`, waits for `release`, and then runs the
    real method. Each later call runs the real method at once.
    """

    def __init__(self, method):
        self.method = method
        self.call_count = 0
        self.started = threading.Event()
        self.release = threading.Event()

    def __call__(self):
        self.call_count += 1
        if self.call_count == 1:
            self.started.set()
            self.release.wait()
        return self.method()


def get_pubsub_client_ids(redis):
    """
    Return the IDs of the pub/sub connections that the tests opened.
    """
    return {
        client["id"]
        for client in redis.client_list(_type="pubsub")
        if client["name"] == TEST_CLIENT_NAME
    }


@pytest.fixture
def short_intervals():
    """
    Make the thread send a heartbeat every 0.1 s, and try again at most 0.1 s
    after each error.
    """
    with patch.multiple(
        redis_hashring,
        POLL_INTERVAL=0.1,
        RETRY_INTERVAL_MIN=0.1,
        RETRY_INTERVAL_MAX=0.1,
    ):
        yield


@pytest.mark.usefixtures("short_intervals")
def test_thread_reports_errors(redis):
    with patch.object(threading, "excepthook") as excepthook:
        with RingNode(redis, TEST_KEY) as node:
            heartbeat = FailingCall(node.heartbeat, failures_left=1)
            node.heartbeat = heartbeat
            wait_for(lambda: heartbeat.success_count > 0)

    excepthook.assert_called_once()
    (args,) = excepthook.call_args.args
    assert args.exc_type is RedisConnectionError
    assert isinstance(args.exc_value, RedisConnectionError)
    assert args.thread.name == f"redis-hashring-{TEST_KEY}"


@pytest.mark.usefixtures("short_intervals")
def test_thread_survives_failed_excepthook(redis):
    with patch.object(
        threading, "excepthook", side_effect=RuntimeError()
    ) as excepthook:
        with RingNode(redis, TEST_KEY) as node:
            heartbeat = FailingCall(node.heartbeat, failures_left=1)
            node.heartbeat = heartbeat

            # The thread tries again, and the next heartbeat works.
            wait_for(lambda: heartbeat.success_count > 0)
            assert node._polling_thread.is_alive()

    excepthook.assert_called_once()


# Keep the default `POLL_INTERVAL`, so that no heartbeat makes the node read
# the ring during the test.
@ignore_thread_errors
@patch.multiple(
    redis_hashring,
    RETRY_INTERVAL_MIN=0.1,
    RETRY_INTERVAL_MAX=0.1,
)
def test_thread_survives_closed_pubsub_connection(redis):
    with RingNode(redis, TEST_KEY) as node1:
        resubscribed = threading.Event()
        ring_read_after_resubscribe = threading.Event()
        original_subscribe = node1._subscribe
        original_update = node1.update

        def subscribe():
            pubsub = original_subscribe()
            resubscribed.set()
            return pubsub

        def update():
            original_update()
            if resubscribed.is_set():
                ring_read_after_resubscribe.set()

        node1._subscribe = subscribe
        node1.update = update

        client_ids = get_pubsub_client_ids(redis)
        assert len(client_ids) == 1, client_ids
        (client_id,) = client_ids
        redis.client_kill_filter(_id=client_id)

        # The thread subscribes again, on a new connection, and reads the
        # ring.
        assert ring_read_after_resubscribe.wait(timeout=5)
        assert get_pubsub_client_ids(redis) - {client_id}

        # Only a notice on the new subscription can make node 1 read the
        # ring again, and see node 2 join.
        with patch.object(socket, "gethostname", return_value="host2"):
            node2 = RingNode(redis, TEST_KEY)
        node2.heartbeat()
        wait_for(lambda: node1.get_node_count() == 2)
        node2.remove()


@ignore_thread_errors
@pytest.mark.usefixtures("short_intervals")
def test_thread_survives_failed_heartbeat(redis):
    with RingNode(redis, TEST_KEY) as node:
        ranges = node.get_ranges()
        heartbeat = FailingCall(node.heartbeat, failures_left=1)
        node.heartbeat = heartbeat

        # The thread tries again, and the next heartbeat works.
        wait_for(lambda: heartbeat.success_count > 0)
        assert heartbeat.failure_count == 1
        assert node._polling_thread.is_alive()
        assert node.get_ranges() == ranges


@ignore_thread_errors
@pytest.mark.usefixtures("short_intervals")
@patch.object(redis_hashring, "NODE_TIMEOUT", 0.5)
def test_node_gives_up_keys_without_heartbeats(redis):
    with RingNode(redis, TEST_KEY) as node:
        ranges = node.get_ranges()
        heartbeat = FailingCall(node.heartbeat, failures_left=math.inf)
        node.heartbeat = heartbeat

        # After 0.4 s without a heartbeat, the node owns no keys.
        wait_for(lambda: node.get_ranges() == ())
        assert node.get_node_count() == 0
        assert node.contains("item") is False

        # When the heartbeat works again, the node gets its keys back.
        heartbeat.heal()
        wait_for(lambda: node.get_ranges() == ranges)


@pytest.mark.usefixtures("short_intervals")
@patch.object(redis_hashring, "NODE_TIMEOUT", 0.5)
def test_node_gives_up_keys_while_heartbeat_blocks(redis):
    with RingNode(redis, TEST_KEY) as node:
        ranges = node.get_ranges()
        heartbeat = BlockingCall(node.heartbeat)
        node.heartbeat = heartbeat
        # The number of heartbeats at each notice.
        notices = []
        original_notify = node._notify

        def notify():
            notices.append(heartbeat.call_count)
            original_notify()

        node._notify = notify

        try:
            assert heartbeat.started.wait(timeout=5)
            # The thread waits in `heartbeat`. After the deadline of 0.4 s,
            # readers see no keys.
            wait_for(lambda: node.get_ranges() == ())
            assert node.get_node_count() == 0
            assert node.contains("item") is False
        finally:
            heartbeat.release.set()

        # The heartbeat ends after the deadline, so the node notifies the
        # other nodes right after it. Then the node gets its keys back.
        wait_for(lambda: node.get_ranges() == ranges)
        # The first notice came right after the heartbeat that blocked.
        assert notices[0] == 1


@ignore_thread_errors
@pytest.mark.usefixtures("short_intervals")
@patch.object(redis_hashring, "NODE_TIMEOUT", 0.5)
def test_node_gives_up_keys_while_update_fails(redis):
    with RingNode(redis, TEST_KEY) as node:
        ranges = node.get_ranges()
        # Count the heartbeats. All of them work.
        heartbeat = FailingCall(node.heartbeat, failures_left=0)
        node.heartbeat = heartbeat
        fetch_ring = FailingCall(node._fetch_ring, failures_left=math.inf)
        node._fetch_ring = fetch_ring

        # No `update` works after the heartbeats. After 0.4 s, the node owns
        # no keys.
        wait_for(lambda: node.get_ranges() == ())
        # More heartbeats without a successful `update` do not give the keys
        # back.
        heartbeat_count = heartbeat.success_count
        wait_for(lambda: heartbeat.success_count >= heartbeat_count + 3)
        assert node.get_ranges() == ()
        assert node.get_node_count() == 0
        assert node.contains("item") is False

        # When `update` works again, the node gets its keys back.
        fetch_ring.heal()
        wait_for(lambda: node.get_ranges() == ranges)


@ignore_thread_errors
@pytest.mark.usefixtures("short_intervals")
@patch.object(redis_hashring, "NODE_TIMEOUT", 0.5)
def test_thread_resets_deadline_when_it_exits(redis):
    node = RingNode(redis, TEST_KEY)
    with node:
        node.heartbeat = FailingCall(node.heartbeat, failures_left=math.inf)
        # The deadline passes, so readers see no keys.
        wait_for(lambda: node.get_ranges() == ())
    # Use the real `heartbeat` again.
    del node.heartbeat

    # Without the thread, no deadline applies to the state.
    node.heartbeat()
    node.update()
    assert node.get_node_count() == 1
    assert node.contains("item") is True


@ignore_thread_errors
@pytest.mark.usefixtures("short_intervals")
@patch.object(redis_hashring, "NODE_TIMEOUT", 0.5)
def test_node_notifies_other_nodes_when_it_joins_again(redis):
    pubsub = redis.pubsub()
    try:
        pubsub.subscribe(TEST_KEY)
        assert pubsub.get_message(timeout=5)["type"] == "subscribe"

        with RingNode(redis, TEST_KEY) as node:
            heartbeat = FailingCall(node.heartbeat, failures_left=math.inf)
            node.heartbeat = heartbeat
            wait_for(lambda: node.get_ranges() == ())
            # Drop the notices from before the outage.
            while pubsub.get_message() is not None:
                pass

            # No cleanup removed the replicas of the node, so the heartbeat
            # only updates their scores. The node sends the notice itself.
            heartbeat.heal()
            message = pubsub.get_message(timeout=5)
            assert message is not None
            assert message["type"] == "message"
    finally:
        pubsub.close()


@pytest.mark.usefixtures("short_intervals")
def test_node_reads_ring_after_each_heartbeat(redis):
    with RingNode(redis, TEST_KEY) as node:
        # Another node joins, and the notice gets lost.
        redis.zadd(TEST_KEY, {f"0:{OTHER_REPLICA}": time.time()})

        wait_for(lambda: node.get_node_count() == 2)


# Keep the default `POLL_INTERVAL`, so that the first failed `update` comes
# from a notice, not from a heartbeat.
@ignore_thread_errors
@patch.multiple(
    redis_hashring,
    RETRY_INTERVAL_MIN=0.1,
    RETRY_INTERVAL_MAX=0.1,
)
def test_thread_survives_failed_update(redis):
    with RingNode(redis, TEST_KEY) as node1:
        fetch_ring = FailingCall(node1._fetch_ring, failures_left=math.inf)
        node1._fetch_ring = fetch_ring

        # Node 1 cannot read the ring after node 2 joins.
        with patch.object(socket, "gethostname", return_value="host2"):
            node2 = RingNode(redis, TEST_KEY)
        node2.heartbeat()
        wait_for(lambda: fetch_ring.failure_count > 0)
        assert node1.get_node_count() == 1

        # The thread tries again, and reads the ring.
        fetch_ring.heal()
        wait_for(lambda: node1.get_node_count() == 2)
        assert node1._polling_thread.is_alive()
        node2.remove()


@ignore_thread_errors
@patch.object(redis_hashring, "POLL_INTERVAL", 0.1)
@patch.object(redis_hashring, "RETRY_INTERVAL_MIN", 10)
def test_stop_while_heartbeat_fails(redis):
    node = RingNode(redis, TEST_KEY)
    node.start()
    try:
        retry_wait_started = threading.Event()
        original_wait_before_retry = node._wait_before_retry

        def wait_before_retry(retry_interval):
            retry_wait_started.set()
            original_wait_before_retry(retry_interval)

        node._wait_before_retry = wait_before_retry
        node.heartbeat = FailingCall(node.heartbeat, failures_left=math.inf)
        # After the first failure, the thread waits at least 5 s before it
        # tries again.
        assert retry_wait_started.wait(timeout=5)
    except BaseException:
        # Stop the thread, so that it does not affect the next tests.
        node.stop()
        raise

    started_at = time.monotonic()
    node.stop()
    assert time.monotonic() - started_at < 2


@ignore_thread_errors
@pytest.mark.usefixtures("short_intervals")
def test_thread_exits_when_stop_arrives_during_subscription(redis):
    with RingNode(redis, TEST_KEY) as node:
        original_subscribe = node._subscribe

        def subscribe():
            # `stop` asks the thread to exit while the thread subscribes.
            node._stop_requested.set()
            return original_subscribe()

        node._subscribe = subscribe
        # The failed heartbeat makes the thread subscribe again.
        heartbeat = FailingCall(node.heartbeat, failures_left=1)
        node.heartbeat = heartbeat

        # The thread exits after the subscription, and sends no heartbeat.
        join_thread(node._polling_thread)
        assert heartbeat.failure_count == 1
        assert heartbeat.success_count == 0


# Keep the default `POLL_INTERVAL`, and make `STOP_CHECK_INTERVAL` long. Then
# the wait for a notice after the heartbeat is longer than the time limit of
# `join_thread`.
@ignore_thread_errors
@patch.multiple(
    redis_hashring,
    STOP_CHECK_INTERVAL=10,
    RETRY_INTERVAL_MIN=0.1,
    RETRY_INTERVAL_MAX=0.1,
)
def test_thread_exits_when_stop_arrives_during_heartbeat(redis):
    node = RingNode(redis, TEST_KEY)
    node.start()
    heartbeat = BlockingCall(node.heartbeat)
    update = FailingCall(node.update, failures_left=1)
    try:
        node.heartbeat = heartbeat
        # The failed update makes the thread subscribe again, and then send a
        # heartbeat.
        node.update = update
        redis.publish(TEST_KEY, "*")
        assert heartbeat.started.wait(timeout=5)
    except BaseException:
        # Stop the thread, so that it does not affect the next tests.
        heartbeat.release.set()
        node.stop()
        raise

    stop_thread = start_thread(node.stop)
    try:
        wait_for(node._stop_requested.is_set)
    finally:
        heartbeat.release.set()

    # The thread exits after the heartbeat, before the update and the wait
    # for a notice.
    join_thread(stop_thread)
    assert update.failure_count == 1
    assert update.success_count == 0
