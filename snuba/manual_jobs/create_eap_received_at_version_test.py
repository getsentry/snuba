from collections.abc import Sequence

from snuba.clickhouse.escaping import escape_identifier, escape_string
from snuba.clickhouse.native import ClickhouseResult
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


class CreateEAPReceivedAtVersionTest(Job):
    """Create the S4S2 received_at-versioned EAP downsample experiment."""

    def _on_cluster(self, cluster: ClickhouseCluster) -> str:
        if cluster.is_single_node():
            return ""
        cluster_name = cluster.get_clickhouse_cluster_name()
        assert cluster_name is not None
        return f" ON CLUSTER {escape_string(cluster_name)}"

    def _add_received_at_query(self, cluster: ClickhouseCluster) -> str:
        return (
            f"ALTER TABLE {_SOURCE_TABLE}{self._on_cluster(cluster)} "
            "ADD COLUMN IF NOT EXISTS received_at UInt64"
        )

    def _get_columns_query(self) -> str:
        return f"DESCRIBE TABLE {_SCHEMA_SOURCE_TABLE} SETTINGS describe_include_subcolumns = 0"

    def _create_table_query(
        self, cluster: ClickhouseCluster, columns: Sequence[ColumnDescription]
    ) -> str:
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
            f"CREATE TABLE IF NOT EXISTS {_DESTINATION_TABLE}{self._on_cluster(cluster)} "
            f"({column_definitions}) ENGINE = {engine}"
        )

    def _create_view_query(
        self, cluster: ClickhouseCluster, columns: Sequence[ColumnDescription]
    ) -> str:
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
        select_columns.append("received_at")
        return (
            f"CREATE MATERIALIZED VIEW IF NOT EXISTS {_VIEW_NAME}{self._on_cluster(cluster)} "
            f"TO {_DESTINATION_TABLE} AS SELECT {', '.join(select_columns)} "
            f"FROM {_SOURCE_TABLE} WHERE (cityHash64(item_id) % 8) = 0"
        )

    def execute(self, logger: JobLogger) -> None:
        cluster = get_cluster(StorageSetKey.EVENTS_ANALYTICS_PLATFORM)
        connection = cluster.get_node_connection(
            ClickhouseClientSettings.MIGRATE, cluster.get_local_nodes()[0]
        )

        add_column_query = self._add_received_at_query(cluster)
        logger.info(f"Executing query: {add_column_query}")
        connection.execute(query=add_column_query)

        columns_result: ClickhouseResult = connection.execute(query=self._get_columns_query())
        columns = [
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
        queries = [
            self._create_table_query(cluster, columns),
            self._create_view_query(cluster, columns),
        ]
        for query in queries:
            logger.info(f"Executing query: {query}")
            connection.execute(query=query)

        logger.info("complete")
