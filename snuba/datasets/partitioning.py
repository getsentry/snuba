from typing import TypeVar

from uhashring import HashRing

"""
The number of logical partitions used to distinguish between where records
should be stored. These do not require individual physical partitions but allow
for repartitioning with less code changes per physical change.
"""
SENTRY_LOGICAL_PARTITIONS = 256

LOGICAL_PARTITION = TypeVar("LOGICAL_PARTITION", bound=int)

# NODE can represent anything which can be uniquely identified in a hash ring.
# In the context of Snuba, this is a string representing the storage set which
# should be used for querying data.
NODE = TypeVar("NODE", bound=str)


def map_org_id_to_logical_partition(org_id: int) -> int:
    """
    Maps an organization id to a logical partition.
    """
    return org_id % SENTRY_LOGICAL_PARTITIONS


def map_logical_partition_to_node(
    logical_partition: LOGICAL_PARTITION, nodes: HashRing
) -> NODE:
    """
    Given a hash ring containing all the nodes on which the partition could
    exist, returns the node that the logical partition is assigned to.

    We use consistent hashing to ensure minimal data movement of logical
    partitions when nodes are added or removed from the cluster.
    """
    return nodes.get_node(str(logical_partition))
