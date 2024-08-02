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
    WHERE database in ('default')
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


class ColumnSizeOnDisk(SystemQuery):
    """
    Shows the size of each column on disk in the selected table. Can be used to compare
    the actual size on disk (compressed) compared to the actual size of the data (uncompressed)
    """

    sql = """
    SELECT
        table,
        column,
        formatReadableSize(sum(column_data_compressed_bytes) AS size) AS compressed,
        formatReadableSize(sum(column_data_uncompressed_bytes) AS usize) AS uncompressed,
        round(usize / size, 2) AS compr_ratio,
        sum(rows) rows_cnt,
        round(usize / rows_cnt, 2) avg_row_size
    FROM system.parts_columns
    WHERE (active = 1) AND (table = '{{table}}')
    GROUP BY
        table,
        column
    ORDER BY size DESC;
    """


class IndexSizeOnDisk(SystemQuery):
    """
    Lists the data skipping indexes as well as their sizes on disk. This table has no
    sense of time, so it's just a snapshot of the current state.
    """

    sql = """
    SELECT
        name,
        type_full,
        expr,
        granularity,
        formatReadableSize(data_compressed_bytes AS size) AS compressed,
        formatReadableSize(data_uncompressed_bytes AS usize) AS uncompressed,
        round(usize / size, 2) AS compr_ratio,
        marks
    FROM system.data_skipping_indices
    WHERE table = '{{table}}'
    """


class PartCreations(SystemQuery):
    """
    New parts created in the last 10 minutes. Replicas have event_type 'DownloadPart',
    therefore we'll only see the new parts on the node that got the insert
    """

    sql = """
    SELECT
        hostName() AS node_name,
        event_time,
        part_name,
        rows,
        size_in_bytes AS bytes_on_disk,
        partition_id,
        part_type
    FROM
        system.part_log
    WHERE
        database = 'default'
        AND table = '{{table}}'
        AND event_time > now() - toIntervalMinute(10)
        and event_type = 'NewPart'
    """


class AsyncInsertPendingFlushes(SystemQuery):
    """
    Current number of aysnc insert queries that are waiting to be flushed
    """

    # Insert is a keyword that isn't allowed, hence using 'like'
    sql = """
    SELECT
        value as queries
    FROM system.metrics
    WHERE metric like 'PendingAsyncInser%'
    """


class AsyncInsertFlushes(SystemQuery):
    """
    Async insert queries (flushes) that happened in the last 10 minutes,
    with a successful status
    """

    sql = """
    SELECT
        table,
        query,
        format,
        query_id,
        bytes,
        flush_time,
        flush_query_id
    FROM
        system.asynchronous_insert_log
    WHERE
        status = 'Ok'
        AND database = 'default'
        AND flush_time > now() - toIntervalMinute(10)
    ORDER BY table, flush_time
    """


class AsyncInsertFlushErrors(SystemQuery):
    """
    Async insert queries that failed within the last 10 minutes
    """

    sql = """
    SELECT
        max(event_time) AS flush,
        status,
        exception,
        any(query_id) AS query_id
    FROM
        system.asynchronous_insert_log
    WHERE
        database = 'default'
        AND status <> 'Ok'
        AND table = '{{table}}'
        AND flush_time > now() - toIntervalMinute(10)
    GROUP BY status, exception
    ORDER BY
        flush DESC
    """
