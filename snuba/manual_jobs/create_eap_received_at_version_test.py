from collections.abc import Sequence

from snuba.clickhouse.escaping import escape_identifier, escape_string
from snuba.clickhouse.native import ClickhousePool, ClickhouseResult
from snuba.clusters.cluster import ClickhouseClientSettings, ClickhouseCluster, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.manual_jobs import Job, JobLogger
from snuba.migrations.table_engines import ReplacingMergeTree

_SOURCE_TABLE = "eap_items_1_local"
_SCHEMA_SOURCE_TABLE = "eap_items_1_downsample_8_local"
_DESTINATION_TABLE = "eap_items_1_downsample_8_timestamp_versioned_test_local"
_VIEW_NAME = "eap_items_1_downsample_8_timestamp_versioned_test_mv"

ColumnDescription = tuple[str, str, str, str, str, str, str]


def _escape_identifier(identifier: str) -> str:
    escaped = escape_identifier(identifier)
    assert escaped is not None
    return escaped


def _column_definition(column: ColumnDescription) -> str:
    name, column_type, default_type, default_expression, comment, codec, ttl = column
    clauses = [f"{_escape_identifier(name)} {column_type}"]
    if default_type:
        clauses.append(f"{default_type} {default_expression}")
    if comment:
        clauses.append(f"COMMENT {escape_string(comment)}")
    if codec:
        clauses.append(f"CODEC({codec})")
    if ttl:
        clauses.append(f"TTL {ttl}")
    return " ".join(clauses)


def _on_cluster(cluster: ClickhouseCluster) -> str:
    if cluster.is_single_node():
        return ""
    cluster_name = cluster.get_clickhouse_cluster_name()
    assert cluster_name is not None
    return f" ON CLUSTER {escape_string(cluster_name)}"


def _get_connection() -> tuple[ClickhouseCluster, ClickhousePool]:
    cluster = get_cluster(StorageSetKey.EVENTS_ANALYTICS_PLATFORM)
    connection = cluster.get_node_connection(
        ClickhouseClientSettings.MIGRATE, cluster.get_local_nodes()[0]
    )
    return cluster, connection


def _get_columns(connection: ClickhousePool, table_name: str) -> list[ColumnDescription]:
    query = f"DESCRIBE TABLE {table_name} SETTINGS describe_include_subcolumns = 0"
    columns_result: ClickhouseResult = connection.execute(query=query)
    return [
        (
            str(name),
            str(column_type),
            str(default_type),
            str(default_expression),
            str(comment),
            str(codec),
            str(ttl),
        )
        for name, column_type, default_type, default_expression, comment, codec, ttl in columns_result.results
    ]


def _add_received_at_query(cluster: ClickhouseCluster) -> str:
    return (
        f"ALTER TABLE {_SOURCE_TABLE}{_on_cluster(cluster)} "
        "ADD COLUMN IF NOT EXISTS received_at UInt64"
    )


def _create_table_query(cluster: ClickhouseCluster, columns: Sequence[ColumnDescription]) -> str:
    assert columns, f"{_SCHEMA_SOURCE_TABLE} has no columns"
    column_definitions = ", ".join(_column_definition(column) for column in columns)
    column_definitions += ", received_at UInt64"
    engine = ReplacingMergeTree(
        storage_set=StorageSetKey.EVENTS_ANALYTICS_PLATFORM,
        version_column="received_at",
        primary_key="(organization_id, project_id, item_type, timestamp)",
        order_by=("(organization_id, project_id, item_type, timestamp, trace_id, item_id)"),
        partition_by="(retention_days, toMonday(timestamp))",
        ttl="timestamp + toIntervalDay(retention_days)",
        settings={
            "index_granularity": "8192",
            "enable_block_number_column": "1",
            "enable_block_offset_column": "1",
        },
    ).get_sql(cluster, _DESTINATION_TABLE)
    return (
        f"CREATE TABLE IF NOT EXISTS {_DESTINATION_TABLE}{_on_cluster(cluster)} "
        f"({column_definitions}) ENGINE = {engine}"
    )


def _create_view_query(cluster: ClickhouseCluster, columns: Sequence[ColumnDescription]) -> str:
    assert columns, f"{_DESTINATION_TABLE} has no columns"
    projections = {
        "retention_days": "downsampled_retention_days AS retention_days",
        "sampling_weight": "sampling_weight * 8 AS sampling_weight",
        "sampling_factor": "sampling_factor / 8 AS sampling_factor",
        "client_sample_rate": "client_sample_rate / 8 AS client_sample_rate",
        "server_sample_rate": "server_sample_rate / 8 AS server_sample_rate",
    }
    select_columns = [
        projections.get(column[0], _escape_identifier(column[0])) for column in columns
    ]
    return (
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS {_VIEW_NAME}{_on_cluster(cluster)} "
        f"TO {_DESTINATION_TABLE} AS SELECT {', '.join(select_columns)} "
        f"FROM {_SOURCE_TABLE} WHERE received_at != 0 AND (cityHash64(item_id) % 8) = 0"
    )


class AddEAPReceivedAtColumn(Job):
    """Add the received_at source column before consumer deployment."""

    def execute(self, logger: JobLogger) -> None:
        cluster, connection = _get_connection()
        query = _add_received_at_query(cluster)
        logger.info(f"Executing query: {query}")
        connection.execute(query=query)
        logger.info("complete")


class CreateEAPReceivedAtVersionTestTable(Job):
    """Create the empty received_at-versioned treatment table."""

    def execute(self, logger: JobLogger) -> None:
        cluster, connection = _get_connection()
        columns = _get_columns(connection, _SCHEMA_SOURCE_TABLE)
        query = _create_table_query(cluster, columns)
        logger.info(f"Executing query: {query}")
        connection.execute(query=query)
        logger.info("complete")


class CreateEAPReceivedAtVersionTestMaterializedView(Job):
    """Start treatment inserts after received_at population is enabled."""

    def execute(self, logger: JobLogger) -> None:
        cluster, connection = _get_connection()
        columns = _get_columns(connection, _DESTINATION_TABLE)
        query = _create_view_query(cluster, columns)
        logger.info(f"Executing query: {query}")
        connection.execute(query=query)
        logger.info("complete")
