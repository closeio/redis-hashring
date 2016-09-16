
from mock import Mock
import pytest
from redis import StrictRedis
from fakeredis import FakeStrictRedis


from redis_hashring import RingNode

TEST_KEY = 'hashring-test'


class TestHashRingFunctional(object):

    def teardown(self):
        self.redis.delete(TEST_KEY)

    def get_node(self, n_replicas, total_replicas):
        node = RingNode(self.redis, TEST_KEY, n_replicas=n_replicas)

        assert len(node.replicas) == n_replicas
        assert self.redis.zcard(TEST_KEY) == total_replicas-n_replicas

        node.heartbeat()

        assert self.redis.zcard(TEST_KEY) == total_replicas
        assert len(node.ranges) == 0

        return node

    def ring_assertions(self):
        node1 = self.get_node(1, 1)
        node1.update()
        assert len(node1.ranges) == 1

        node2 = self.get_node(1, 2)
        node1.update()
        node2.update()
        assert len(node1.ranges) + len(node2.ranges) == 3

        node3 = self.get_node(2, 4)
        node1.update()
        node2.update()
        node3.update()
        assert len(node1.ranges) + len(node2.ranges) + len(node3.ranges) == 5

    @pytest.mark.requires_redis_server
    def test_with_redis_server(self):
        self.redis = StrictRedis()
        self.ring_assertions()

    def test_with_fake_redis_server(self, monkeypatch):
        self.redis = FakeStrictRedis()

        # mock publish, since fakeredis doesn't support
        self.redis.publish = Mock()

        self.ring_assertions()
