import pytest
from uhashring import HashRing

from snuba.datasets.partitioning import (
    SENTRY_LOGICAL_PARTITIONS,
    map_logical_partition_to_node,
)

# Allow 15 percent of deviation from the ideal scenario of partitions which
# are moved to a different node.
PARTITION_MOVE_DEVIATION_ALLOWED = 0.15


@pytest.mark.parametrize(
    "nodes,new_hash_ring,partition_moves_allowed",
    [
        pytest.param(
            HashRing(
                nodes=["node1"],
            ),
            HashRing(
                nodes=["node1", "node2"],
            ),
            SENTRY_LOGICAL_PARTITIONS / 2
            + PARTITION_MOVE_DEVIATION_ALLOWED * SENTRY_LOGICAL_PARTITIONS / 2,
            id="one to two nodes",
        ),
        pytest.param(
            HashRing(
                nodes=["node1", "node2"],
            ),
            HashRing(
                nodes=["node1", "node2", "node3"],
            ),
            SENTRY_LOGICAL_PARTITIONS / 3
            + PARTITION_MOVE_DEVIATION_ALLOWED * SENTRY_LOGICAL_PARTITIONS / 3,
            id="two to three nodes",
        ),
        pytest.param(
            HashRing(
                nodes=["node1", "node2", "node3"],
            ),
            HashRing(
                nodes=["node1", "node2", "node3", "node4"],
            ),
            SENTRY_LOGICAL_PARTITIONS / 4
            + PARTITION_MOVE_DEVIATION_ALLOWED * SENTRY_LOGICAL_PARTITIONS / 4,
            id="three to four nodes",
        ),
        pytest.param(
            HashRing(
                nodes=["node1", "node2", "node3", "node4"],
            ),
            HashRing(
                nodes=["node1", "node2", "node3", "node4", "node5"],
            ),
            SENTRY_LOGICAL_PARTITIONS / 5
            + PARTITION_MOVE_DEVIATION_ALLOWED * SENTRY_LOGICAL_PARTITIONS / 5,
            id="four to five nodes",
        ),
        pytest.param(
            HashRing(
                nodes=["node1", "node2", "node3", "node4", "node5"],
            ),
            HashRing(
                nodes=["node1", "node2", "node3", "node4"],
            ),
            SENTRY_LOGICAL_PARTITIONS / 5
            + PARTITION_MOVE_DEVIATION_ALLOWED * SENTRY_LOGICAL_PARTITIONS / 5,
            id="five to four nodes",
        ),
        pytest.param(
            HashRing(
                nodes=["node1", "node2", "node3", "node4"],
            ),
            HashRing(
                nodes=["node1", "node2", "node3"],
            ),
            SENTRY_LOGICAL_PARTITIONS / 4
            + PARTITION_MOVE_DEVIATION_ALLOWED * SENTRY_LOGICAL_PARTITIONS / 4,
            id="four to three nodes",
        ),
        pytest.param(
            HashRing(
                nodes=["node1", "node2", "node3"],
            ),
            HashRing(
                nodes=["node1", "node2"],
            ),
            SENTRY_LOGICAL_PARTITIONS / 3
            + PARTITION_MOVE_DEVIATION_ALLOWED * SENTRY_LOGICAL_PARTITIONS / 3,
            id="three to two nodes",
        ),
        pytest.param(
            HashRing(
                nodes=["node1", "node2"],
            ),
            HashRing(
                nodes=["node1"],
            ),
            SENTRY_LOGICAL_PARTITIONS / 2
            + PARTITION_MOVE_DEVIATION_ALLOWED * SENTRY_LOGICAL_PARTITIONS / 2,
            id="two to one nodes",
        ),
    ],
)
def test_partition_rebalancing(hash_ring, new_hash_ring, partition_moves_allowed):
    """
    Test that when a node is added to the hash ring, the number of partitions
    which are moved around in total is within a certain threshold.
    """
    original_partition_node_map = {}
    for partition in range(SENTRY_LOGICAL_PARTITIONS):
        original_partition_node_map[partition] = map_logical_partition_to_node(
            partition, hash_ring
        )

    new_partition_node_map = {}
    for partition in range(SENTRY_LOGICAL_PARTITIONS):
        new_partition_node_map[partition] = map_logical_partition_to_node(
            partition, new_hash_ring
        )

    difference = len(
        set(new_partition_node_map.items()) - set(original_partition_node_map.items())
    )
    assert difference < partition_moves_allowed
