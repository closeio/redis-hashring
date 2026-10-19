"""
Print the replicas and the nodes of a hash ring.

Run `python -m redis_hashring --help` for the arguments.
"""

import argparse
import collections
import operator
import os
import time

from redis import Redis

from redis_hashring._ring_node import NODE_TIMEOUT, RING_SIZE, _decode

# The environment variable that holds the URL of the Redis server. A URL in
# the environment does not show in the arguments of the process.
_URL_VARIABLE = "REDIS_HASHRING_URL"

# The URL of the Redis server if `--url` is not set, and the environment
# variable is not set or is empty.
_DEFAULT_URL = "redis://localhost:6379/0"

# The socket timeout of the command, in seconds. The README states this value.
# If you change it, change the text too.
_SOCKET_TIMEOUT = 10


def _parse_arguments(argv):
    parser = argparse.ArgumentParser(
        prog="python -m redis_hashring",
        description="Print the replicas and the nodes of a hash ring.",
    )
    parser.add_argument("key", help="The Redis key of the ring.")
    parser.add_argument(
        "--url",
        # Do not show this default in the help. It can hold a password.
        default=os.environ.get(_URL_VARIABLE) or _DEFAULT_URL,
        help=(
            "The URL of the Redis server. Default: the environment variable"
            f" {_URL_VARIABLE}, or {_DEFAULT_URL} if the variable is not set"
            " or is empty."
        ),
    )
    return parser.parse_args(argv)


def _connect(url):
    """
    Return a Redis client for the server at `url`.

    The client waits up to `_SOCKET_TIMEOUT` seconds for Redis to accept the
    connection, and then for each reply. If the URL sets `socket_timeout`,
    the client uses that value instead.
    """
    return Redis.from_url(url, socket_timeout=_SOCKET_TIMEOUT)


def _describe_server(conn):
    """
    Return the address and the database of the Redis server of `conn`.

    The result holds no user name and no password.
    """
    connection_kwargs = conn.connection_pool.connection_kwargs
    # A URL with the scheme `unix://` gives a path, not a host and a port.
    address = connection_kwargs.get("path")
    if address is None:
        # Show the defaults of the Redis client for the values that the URL
        # leaves out.
        host = connection_kwargs.get("host") or "localhost"
        port = connection_kwargs.get("port") or 6379
        address = f"{host}:{port}"
    database = connection_kwargs.get("db") or 0
    return f"{address}, database {database}"


def _fetch_ring_all(conn, key):
    """
    Fetch the ring from Redis.

    The fetched ring will include inactive nodes. Returns a list of tuples
    (start, replica, heartbeat, expired), where:
    * start: start of the range for which the replica is responsible.
    * replica: name of the replica.
    * heartbeat: timestamp of the last heartbeat.
    * expired: boolean denoting whether this replica is inactive.
    """
    expiry_time = time.time() - NODE_TIMEOUT
    data = conn.zrange(key, 0, -1, withscores=True)

    ring = []
    for replica_data, heartbeat in data:
        start, replica = _decode(replica_data).split(":", 1)
        ring.append((int(start), replica, heartbeat, heartbeat < expiry_time))
    return sorted(ring, key=operator.itemgetter(0))


def _print_ring(conn, key):
    """
    Print the Redis server of `conn`, and the replicas and the nodes of the
    ring.
    """
    # Print the server first. If the read of the ring fails, the output still
    # names the server.
    print(f"Redis server: {_describe_server(conn)}")
    print()

    ring = _fetch_ring_all(conn, key)

    print('Hash ring "{key}" replicas:'.format(key=key))

    now = time.time()

    n_replicas = len(ring)
    if ring:
        print("{:10} {:6} {:7} {}".format("Start", "Range", "Delay", "Node"))
    else:
        print("(no replicas)")

    nodes = collections.defaultdict(list)

    for n, (start, replica, heartbeat, expired) in enumerate(ring):
        hostname, pid, _ = replica.split(":")
        node = ":".join([hostname, pid])

        next_start = ring[(n + 1) % n_replicas][0]
        # If the next start is equal to this start, the replica owns the
        # whole ring.
        abs_size = (next_start - start) % RING_SIZE or RING_SIZE
        size = 100.0 / RING_SIZE * abs_size
        delay = int(now - heartbeat)
        expired_str = "(EXPIRED)" if expired else ""

        nodes[node].append((hostname, pid, abs_size, delay, expired))

        print(f"{start:10} {size:5.2f}% {delay:6}s {replica} {expired_str}")

    print()
    print('Hash ring "{key}" nodes:'.format(key=key))

    if nodes:
        print(
            "{:8} {:8} {:7} {:20} {:5}".format(
                "Range", "Replicas", "Delay", "Hostname", "PID"
            )
        )
    else:
        print("(no nodes)")

    for _, v in nodes.items():
        hostname, pid = v[0][0], v[0][1]
        abs_size = sum(replica[2] for replica in v)
        size = 100.0 / RING_SIZE * abs_size
        delay = max(replica[3] for replica in v)
        expired = any(replica[4] for replica in v)
        count = len(v)
        expired_str = "(EXPIRED)" if expired else ""
        print(
            f"{size:5.2f}% {count:8} {delay:6}s {hostname:20} {pid:5}"
            f" {expired_str}"
        )


def _main(argv=None):
    """
    Parse the arguments in `argv`, and print the ring.

    If `argv` is `None`, read the arguments from `sys.argv`.
    """
    args = _parse_arguments(argv)
    _print_ring(_connect(args.url), args.key)


if __name__ == "__main__":
    _main()
