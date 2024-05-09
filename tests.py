import socket
from unittest.mock import patch

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

    assert len(node._replicas) == n_replicas
    assert redis.zcard(TEST_KEY) == total_replicas - n_replicas

    node.heartbeat()

    assert redis.zcard(TEST_KEY) == total_replicas
    assert len(node._ranges) == 0

    return node


def test_node(redis):
    with patch.object(socket, "gethostname", return_value="host1"):
        node1 = get_node(redis, 1, 1)
    node1.update()
    assert len(node1._ranges) == 1
    assert node1.get_node_count() == 1

    with patch.object(socket, "gethostname", return_value="host2"):
        node2 = get_node(redis, 1, 2)
    node1.update()
    node2.update()
    assert len(node1._ranges) + len(node2._ranges) == 3
    assert node1.get_node_count() == 2
    assert node2.get_node_count() == 2

    with patch.object(socket, "gethostname", return_value="host3"):
        node3 = get_node(redis, 2, 4)
    node1.update()
    node2.update()
    node3.update()
    assert len(node1._ranges) + len(node2._ranges) + len(node3._ranges) == 5
    assert node1.get_node_count() == 3
    assert node2.get_node_count() == 3
    assert node3.get_node_count() == 3
