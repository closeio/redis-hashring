import itertools
import socket
import threading
import time
from unittest.mock import Mock, patch

import pytest
from redis import Redis
from redis.client import Pipeline, PubSub
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    TimeoutError as RedisTimeoutError,
)

import redis_hashring
from redis_hashring import (
    RING_SIZE,
    SUBSCRIBE_TIMEOUT_SECONDS,
    HashAlgorithm,
    RingNode,
)

TEST_KEY = "hashring-test"

OTHER_REPLICA = "host2:1:00000000"


@pytest.fixture
def redis():
    redis = Redis()
    yield redis
    redis.delete(TEST_KEY)


@pytest.fixture(autouse=True)
def short_stop_check_interval():
    """
    Make the thread check for a request to stop every 0.05 s.
    """
    with patch.object(redis_hashring, "STOP_CHECK_INTERVAL_SECONDS", 0.05):
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


def lock_is_free(lock):
    """
    Return whether another thread can take the lock.

    The owner of a reentrant lock can always take it again, so the check runs
    on a new thread.
    """
    acquired = []

    def try_acquire():
        if lock.acquire(blocking=False):
            acquired.append(True)
            lock.release()

    join_thread(start_thread(try_acquire))
    return bool(acquired)


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
    assert lock_is_free(node._state_lock)


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

    with patch.object(Pipeline, "execute", side_effect=RedisConnectionError()):
        with pytest.raises(RedisConnectionError):
            node.remove()

    assert node.get_ranges() == ()
    assert node.get_node_count() == 0
    assert lock_is_free(node._state_lock)


def test_remove_during_remove_on_same_thread(redis):
    node = get_node(redis, 1, 1, HashAlgorithm.XXHASH)
    node.update()
    execute = Pipeline.execute
    nested_calls = []

    def execute_with_nested_remove(pipeline):
        # Call `remove` before the ZREM, on the same thread, like a signal
        # handler that interrupts `remove`.
        if not nested_calls:
            nested_calls.append(True)
            node.remove()
        return execute(pipeline)

    with patch.object(Pipeline, "execute", execute_with_nested_remove):
        join_thread(start_thread(node.remove))

    assert nested_calls
    assert redis.zcard(TEST_KEY) == 0
    assert node.get_ranges() == ()
    assert node.get_node_count() == 0
    assert lock_is_free(node._state_lock)


def test_remove_clears_state_after_update_on_same_thread(redis):
    node = get_node(redis, 1, 1, HashAlgorithm.XXHASH)
    execute = Pipeline.execute
    nested_ranges = []

    def execute_with_nested_update(pipeline):
        # Call `update` before the ZREM, on the same thread, like a signal
        # handler that interrupts `remove`.
        if not nested_ranges:
            node.update()
            nested_ranges.append(node.get_ranges())
        return execute(pipeline)

    with patch.object(Pipeline, "execute", execute_with_nested_update):
        join_thread(start_thread(node.remove))

    # The ring still had the node, so the nested `update` published ranges.
    assert nested_ranges == [((0, RING_SIZE),)]
    assert redis.zcard(TEST_KEY) == 0
    assert node.get_ranges() == ()
    assert node.get_node_count() == 0
    assert lock_is_free(node._state_lock)


def test_update_keeps_empty_state_after_remove_on_same_thread(redis):
    node = get_node(redis, 1, 1, HashAlgorithm.XXHASH)
    own_replica = node._replicas[0][1]

    def fetch_ring_with_nested_remove():
        # Call `remove` during the fetch, on the same thread, like a signal
        # handler that interrupts `update`.
        node.remove()
        return [(0, own_replica)]

    node._fetch_ring = fetch_ring_with_nested_remove
    join_thread(start_thread(node.update))

    assert redis.zcard(TEST_KEY) == 0
    assert node.get_ranges() == ()
    assert node.get_node_count() == 0
    assert lock_is_free(node._state_lock)

    # The next update publishes its ranges.
    node._fetch_ring = Mock(return_value=[(0, own_replica)])
    node.update()
    assert node.get_ranges() == ((0, RING_SIZE),)
    assert node.get_node_count() == 1


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
    get_message.assert_called_once_with(timeout=SUBSCRIBE_TIMEOUT_SECONDS)
    assert set(threading.enumerate()) <= threads_before
    assert node._polling_thread is None
    # The setup failed before it wrote to the ring.
    assert redis.zcard(TEST_KEY) == 0


def test_start_twice_raises(redis):
    with RingNode(redis, TEST_KEY) as node:
        with pytest.raises(RuntimeError, match="already started"):
            node.start()

        assert node._polling_thread.is_alive()


def test_start_and_stop_raise_during_start_on_other_thread(redis):
    node = RingNode(redis, TEST_KEY)
    original_heartbeat = node.heartbeat
    setup_paused = threading.Event()
    resume_setup = threading.Event()

    def heartbeat():
        setup_paused.set()
        resume_setup.wait()
        original_heartbeat()

    try:
        with patch.object(node, "heartbeat", side_effect=heartbeat):
            first_start_thread = start_thread(node.start)
            try:
                assert setup_paused.wait(timeout=5)
                with pytest.raises(RuntimeError, match="in progress"):
                    node.stop()
                with pytest.raises(RuntimeError, match="in progress"):
                    node.start()
            finally:
                # Let the first call end, even if an assertion fails.
                resume_setup.set()
                join_thread(first_start_thread)

        # The first call joined the ring.
        assert node._polling_thread.is_alive()
        assert node.get_node_count() == 1
    finally:
        # Stop the node, so that it does not change the ring in the next
        # tests.
        node.stop()

    assert redis.zcard(TEST_KEY) == 0


def test_stop_during_start_on_same_thread_raises(redis):
    node = RingNode(redis, TEST_KEY)
    original_wait = node._setup_done.wait
    nested_errors = []

    def wait_with_nested_stop(*args, **kwargs):
        # Call `stop` once during `start`, on the same thread, like a signal
        # handler that interrupts `start`.
        if not nested_errors:
            try:
                node.stop()
            except RuntimeError as error:
                nested_errors.append(error)
            else:
                nested_errors.append(None)
        return original_wait(*args, **kwargs)

    try:
        with patch.object(
            node._setup_done, "wait", side_effect=wait_with_nested_stop
        ):
            # Run `start` on a new thread, so that a deadlock fails the test.
            join_thread(start_thread(node.start))

        assert len(nested_errors) == 1
        assert isinstance(nested_errors[0], RuntimeError)
        assert "in progress" in str(nested_errors[0])
        # The outer call joined the ring.
        assert node._polling_thread.is_alive()
        assert node.get_node_count() == 1
    finally:
        # Stop the node, so that it does not change the ring in the next
        # tests.
        node.stop()

    assert redis.zcard(TEST_KEY) == 0


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


@patch.object(redis_hashring, "POLL_INTERVAL_SECONDS", 0.1)
def test_node_reads_ring_after_each_heartbeat(redis):
    with RingNode(redis, TEST_KEY) as node:
        # Another node joins, and the notice gets lost.
        redis.zadd(TEST_KEY, {f"0:{OTHER_REPLICA}": time.time()})

        wait_for(lambda: node.get_node_count() == 2)
