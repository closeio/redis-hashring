import itertools
import socket
import threading
from unittest.mock import Mock, patch

import pytest
from redis import Redis
from redis.client import Pipeline
from redis.exceptions import ConnectionError as RedisConnectionError

from redis_hashring import RING_SIZE, HashAlgorithm, RingNode

TEST_KEY = "hashring-test"

OTHER_REPLICA = "host2:1:00000000"


@pytest.fixture
def redis():
    redis = Redis()
    yield redis
    redis.delete(TEST_KEY)


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
