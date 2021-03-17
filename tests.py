import pytest
from redis import Redis
from redis_hashring import RingNode
import select

TEST_KEY = 'hashring-test'


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


def test_drain_pubsub_channel(redis):
    node = RingNode(redis, TEST_KEY, n_replicas=1)

    pubsub = redis.pubsub()
    pubsub.subscribe(node.key)
    gen = pubsub.listen()

    redis.publish(node.key, '*')
    redis.publish(node.key, '*')

    fileno = pubsub.connection._sock.fileno()
    assert select.select([fileno], [], [], 0)[0] == [fileno]
    node._drain_pubsub_channel(gen, fileno)
    assert select.select([fileno], [], [], 0)[0] == []
