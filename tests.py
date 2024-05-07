import pytest
from redis import Redis

from redis_hashring import RingNode

TEST_KEY = "hashring-test"


@pytest.fixture
def redis():
    redis = Redis()
    yield redis
    redis.delete(TEST_KEY)


def get_node(redis, n_replicas, total_replicas):
    node = RingNode(redis, TEST_KEY, n_replicas=n_replicas)

    assert len(node.replicas) == n_replicas
    assert redis.zcard(TEST_KEY) == total_replicas - n_replicas

    node.heartbeat()

    assert redis.zcard(TEST_KEY) == total_replicas
    assert len(node.ranges) == 0

    return node


def test_node(redis):
    node1 = get_node(redis, 1, 1)
    node1.update()
    assert len(node1.ranges) == 1

    node2 = get_node(redis, 1, 2)
    node1.update()
    node2.update()
    assert len(node1.ranges) + len(node2.ranges) == 3

    node3 = get_node(redis, 2, 4)
    node1.update()
    node2.update()
    node3.update()
    assert len(node1.ranges) + len(node2.ranges) + len(node3.ranges) == 5
