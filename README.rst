==============
redis-hashring
==============
.. image:: https://circleci.com/gh/closeio/redis-hashring.svg?style=svg&circle-token=e9b81f0e4bc9a1a0b6150522e854ca0c9b1c2881
    :target: https://circleci.com/gh/closeio/redis-hashring/tree/master

*redis-hashring* is a Python library that implements a consistent hash ring
for building distributed applications, which is stored in Redis.

The problem
-----------

Let's assume you're building a distributed application that's responsible for
syncing accounts. Accounts are synced continuously, e.g. by keeping a
connection open. Given the large amount of accounts, the application can't
run in one process and has to be distributed and split up in multiple
processes. Also, if one of the processes fails or crashes, other machines need
to be able to take over accounts quickly. The load should be balanced equally
between the machines.

The solution
------------

A solution to this problem is to use a consistent hash ring: Different Python
instances ("nodes") are responsible for a different set of keys. In our account
example, the account IDs could be used as keys. A consistent hash ring is a
large (integer) space that wraps around to form a circle. Each node picks a few
random points ("replicas") on the hash ring when starting. Keys are hashed and
looked up on the hash ring: In order to find the node that's responsible for a
given key, we move on the hash ring until we find the next smaller point that
belongs to a replica. The reason for multiple replicas per node is to ensure
better distribution of the keys amongst the nodes. It can also be used to give
certain nodes more weight. The ring is automatically rebalanced when a node
enters or leaves the ring: If a node crashes or shuts down, its replicas are
removed from the ring.

How it works
------------

The ring is stored as a sorted set (ZSET) in Redis. Each replica is a member
of the set, scored by it's expiration time. Each node needs to periodically
refresh the score of its replicas to stay on the ring.

The ring contains 2^32 points, and a replica is created by randomly placing
a point on the ring.  A replica of a node is responsible for the range of
points from its randomly generated starting point until the starting point of
the next node / replica.

To check if a node is responsible for a given key, the key's position on the
ring is determined by hashing the key using CRC-32.

For example, let's say there are two nodes, having one replica each. The first
node is at 1 000 000 000 (1e9), the second at 2e9. In this case, the first node
is responsible for the range [1e9, 2e9-1], the second node is responsible for
[2e9, 2^32-1] and [0, 1e9-1], since the ring wraps. To check if the key
*hello* is on the ring, we compute CRC-32, which is 907 060 870, and the value
is therefore on the first node.

Since the node replica points are picked randomly, it is recommended to have
multiple replicas of the node on a ring to ensure a more even distribution of
the nodes.

Demo
----

As an example, let's assume you have a process that is responsible for syncing
accounts. In this example they are numbered from 0 to 99. Starting node 1 will
assign all accounts to node 1, since it's the only node on the ring.

We can see this by running the provided example script on node 1:

.. code:: bash

  % python example.py
  INFO:root:PID 80721, 100 keys ([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99])

We can print the ring for debugging and see all the nodes and replicas on the
ring:

.. code:: bash

  % python example.py --print
  Hash ring "ring" replicas:
  Start      Range  Delay   Node
   706234936  2.97%      0s mbp.local:80721:249d729d
   833679955  3.58%      0s mbp.local:80721:aa60d44c
   987624694 24.44%      0s mbp.local:80721:aa7d4433
  2037338983  3.41%      0s mbp.local:80721:e810d068
  2183761853  3.55%      0s mbp.local:80721:3917f572
  2336151471  2.82%      0s mbp.local:80721:e42b1b46
  2457297989  4.40%      0s mbp.local:80721:e6bd5726
  2646391033  4.37%      0s mbp.local:80721:6de2fc22
  2834073726  5.30%      0s mbp.local:80721:b6f950b2
  3061910569  3.96%      0s mbp.local:80721:d176c9e2
  3231812046  5.70%      0s mbp.local:80721:65432143
  3476455773  5.71%      0s mbp.local:80721:f2b29682
  3721589736  0.65%      0s mbp.local:80721:51d0cb09
  3749333446  5.53%      0s mbp.local:80721:3572f718
  3986767934  4.39%      0s mbp.local:80721:42147f45
  4175523935 19.22%      0s mbp.local:80721:296c9522

  Hash ring "ring" nodes:
  Range    Replicas Delay   Hostname             PID
  100.00%       16      0s mbp.local            80721

We can see that the node is responsible for the entire ring (range 100%) and
has 16 replicas on the ring.

Now let's start another node by running the script again. It will add its
replicas to the ring and notify all the remaining nodes.

.. code:: bash

  % python example.py
  INFO:root:PID 80721, 51 keys ([1, 5, 8, 9, 10, 14, 17, 20, 21, 24, 25, 28, 30, 32, 33, 34, 36, 38, 41, 42, 45, 46, 49, 50, 52, 54, 56, 58, 59, 60, 61, 62, 65, 66, 68, 69, 71, 74, 75, 78, 79, 81, 82, 85, 86, 87, 88, 89, 92, 93, 96])

Node 1 will rebalance and is now only responsible for keys not in node 2:

.. code:: bash

  INFO:root:PID 80808, 49 keys ([0, 2, 3, 4, 6, 7, 11, 12, 13, 15, 16, 18, 19, 22, 23, 26, 27, 29, 31, 35, 37, 39, 40, 43, 44, 47, 48, 51, 53, 55, 57, 63, 64, 67, 70, 72, 73, 76, 77, 80, 83, 84, 90, 91, 94, 95, 97, 98, 99])

We can inspect the ring:

.. code:: bash

  % python example.py --print
  Hash ring "ring" replicas:
  Start      Range  Delay   Node
   204632062  1.06%      0s mbp.local:80808:f933c33c
   250215779  0.36%      0s mbp.local:80808:3b104c45
   265648189  1.15%      0s mbp.local:80808:84d71125
   315059885  2.77%      0s mbp.local:80808:bab5a03c
   434081415  6.34%      0s mbp.local:80808:6eec1b26
   706234936  2.97%      0s mbp.local:80721:249d729d
   833679955  1.59%      0s mbp.local:80721:aa60d44c
   901926411  2.00%      0s mbp.local:80808:bd6f3b27
   987624694  2.87%      0s mbp.local:80721:aa7d4433
  1110943067  5.42%      0s mbp.local:80808:abfa5d78
  1343923832  0.83%      0s mbp.local:80808:5261947f
  1379658747  4.70%      0s mbp.local:80808:cb0904de
  1581392642  1.06%      0s mbp.local:80808:3050daa3
  1627017290  9.55%      0s mbp.local:80808:8e1cef12
  2037338983  3.41%      0s mbp.local:80721:e810d068
  2183761853  3.55%      0s mbp.local:80721:3917f572
  2336151471  2.82%      0s mbp.local:80721:e42b1b46
  2457297989  4.40%      0s mbp.local:80721:e6bd5726
  2646391033  4.37%      0s mbp.local:80721:6de2fc22
  2834073726  2.30%      0s mbp.local:80721:b6f950b2
  2932842903  3.01%      0s mbp.local:80808:58f09769
  3061910569  3.08%      0s mbp.local:80721:d176c9e2
  3194206736  0.88%      0s mbp.local:80808:ce94a1cf
  3231812046  5.70%      0s mbp.local:80721:65432143
  3476455773  0.21%      0s mbp.local:80721:f2b29682
  3485592199  5.49%      0s mbp.local:80808:6fc107a3
  3721589736  0.65%      0s mbp.local:80721:51d0cb09
  3749333446  0.68%      0s mbp.local:80721:3572f718
  3778349273  4.85%      0s mbp.local:80808:e7cc7485
  3986767934  1.29%      0s mbp.local:80721:42147f45
  4042192844  3.10%      0s mbp.local:80808:001590b5
  4175523935  7.55%      0s mbp.local:80721:296c9522

  Hash ring "ring" nodes:
  Range    Replicas Delay   Hostname             PID
  47.42%       16      0s mbp.local            80721
  52.58%       16      0s mbp.local            80808

gevent example
--------------

*redis-hashring* provides a ``RingNode`` class, which has helper methods for
`gevent`-based applications. The ``RingNode.gevent_start()`` method spawns a
greenlet that initializes the ring and periodically updates the node's
replicas.

An example app could look as follows:

.. code:: python

  from redis import Redis
  from redis_hashring import RingNode

  KEY = 'example-ring'

  redis = Redis()
  node = RingNode(redis, KEY)
  node.gevent_start()

  def get_items():
      """
      Implement this method and return items to be processed.
      """
      raise NotImplementedError()

  def process_items(items):
      """
      Implement this method and process the given items.
      """
      raise NotImplementedError()

  try:
      while True:
          # Only process items this node is reponsible for.
          items = [item for item in get_items() if node.contains(item)]
          process_items(items)
  except KeyboardInterrupt:
      pass

  node.gevent_stop()

Implementation considerations
-----------------------------

When implementing a distributed application using redis-hashring, be aware of
the following:

- Locking

  When nodes are added to the ring, multiple nodes might assume they're
  responsible for the same key until they are notified about the new state of
  the ring. Depending on the application, locking may be necessary to avoid
  duplicate processing.

  For example, in the demo above the node could add a per-account-ID lock if an
  account should never be synced by multiple nodes at the same time. This can
  be done using a Redis lock class or any other distributed lock.

- Limit

  It is recommended to add an upper limit to the number of keys a node can
  process to avoid overloading a node when there are few nodes on the ring or
  all nodes need to be restarted.

  For example, in the demo above we could implement a limit of 50 accounts, if
  we know that a node may not be capable of syncing much more. In this case,
  multiple nodes would need to be running to sync all the accounts. Also note
  that the ring is not usually equally balanced, so running 2 nodes wouldn't be
  enough in this example.
