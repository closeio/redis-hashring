import unittest

import pytest
from redis import Redis

from redis_hashring import RingNode

TEST_KEY = 'hashring-test'


@pytest.mark.requires_redis_server
class HashRingTestCase(unittest.TestCase):

    def setUp(self):
        self.redis = Redis()
        self.redis.delete(TEST_KEY)

    def get_node(self, n_replicas, total_replicas):
        node = RingNode(self.redis, TEST_KEY, n_replicas=n_replicas)

        self.assertEqual(len(node.replicas), n_replicas)
        self.assertEqual(self.redis.zcard(TEST_KEY), total_replicas-n_replicas)

        node.heartbeat()

        self.assertEqual(self.redis.zcard(TEST_KEY), total_replicas)
        self.assertEqual(len(node.ranges), 0)

        return node

    def test_node(self):
        node1 = self.get_node(1, 1)
        node1.update()
        self.assertEqual(len(node1.ranges), 1)

        node2 = self.get_node(1, 2)
        node1.update()
        node2.update()
        self.assertEqual(len(node1.ranges) + len(node2.ranges), 3)

        node3 = self.get_node(2, 4)
        node1.update()
        node2.update()
        node3.update()
        self.assertEqual(len(node1.ranges) + len(node2.ranges) + len(node3.ranges), 5)
