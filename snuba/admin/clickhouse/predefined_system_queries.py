from __future__ import annotations

from snuba.admin.clickhouse.common import PreDefinedQuery
from snuba.utils.registered_class import RegisteredClass


class SystemQuery(PreDefinedQuery, metaclass=RegisteredClass):
    @classmethod
    def config_key(cls) -> str:
        return cls.__name__


class CurrentMerges(SystemQuery):
    """Currently executing merges"""

    sql = """
    SELECT
        table,
        elapsed,
        progress,
        result_part_name
    FROM system.merges
    """


class CreateTableQuery(SystemQuery):
    """Show the current state of the schema by looking at the create_table_query"""

    sql = """
    SELECT
        create_table_query
    FROM system.tables
    WHERE database not in ('system')
    """


class ActivePartsPerTable(SystemQuery):
    """Number of parts grouped by table. Parts should not be in the high thousands."""

    sql = """
    SELECT
        count(),
        partition,
        table
    FROM system.parts
    WHERE active = 1
    GROUP BY table, partition
    ORDER BY count()
    """


class PartitionSizeByTable(SystemQuery):
    """Sum of the size of parts within a partition on a given table."""

    sql = """
    SELECT
        partition,
        table,
        count(),
        formatReadableSize(sum(bytes_on_disk) as bytes) as size
    FROM system.parts
    WHERE active = 1
    GROUP BY partition, table
    ORDER BY partition ASC
    """


class PartSizeWithinPartition(SystemQuery):
    """Gets the size of parts within a specific partition. You'll need to input
    partition = '(90,"2022-01-24")' with the partition you want.

    To get partitions you can use SELECT partition FROM system.parts
    """

    sql = """
    SELECT name, formatReadableSize(bytes_on_disk) as size
    FROM system.parts
    WHERE
        partition = '(90,"2022-01-24")'  AND
        active = 1 AND
        bytes_on_disk > 1000000
    ORDER BY bytes_on_disk DESC
    """


class InactiveReplicas(SystemQuery):
    """
    Checks for inactive replicas by querying for active_replicas < total_replicas.
    Also checks if total_replicas < 2 as this is likely an indication
    that a replica is not configured correctly.

    Shows the replica path of the current replica. To get the replica paths of the
    inactive replicas you need to inspect the system.zookeeper table.

    """

    sql = """
    SELECT
        total_replicas,
        active_replicas,
        replica_path,
        last_queue_update,
        zookeeper_exception,
        is_leader,
        is_readonly
    FROM system.replicas
    WHERE
        active_replicas < total_replicas
        OR total_replicas < 2
    """
