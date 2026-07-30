from collections.abc import Mapping
from typing import Any

from snuba.clickhouse.escaping import escape_identifier
from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.manual_jobs import Job, JobLogger, JobSpec

_DOWNSAMPLED_EAP_TABLES = [
    "eap_items_1_downsample_8_local",
    "eap_items_1_downsample_64_local",
    "eap_items_1_downsample_512_local",
]


class SetEAPDownsampledTableSettings(Job):
    """Enable block metadata columns on a downsampled EAP local table."""

    def __init__(self, job_spec: JobSpec) -> None:
        self.__validate_job_params(job_spec.params)
        super().__init__(job_spec)

    def __validate_job_params(self, params: Mapping[Any, Any] | None) -> None:
        assert params is not None, "table_name parameter required"
        table_name = params.get("table_name")
        assert isinstance(table_name, str) and table_name in _DOWNSAMPLED_EAP_TABLES, (
            "table_name must be a downsampled EAP local table"
        )
        self._table_name = table_name

    def _get_query(self, cluster_name: str | None) -> str:
        on_cluster = f" ON CLUSTER '{cluster_name}'" if cluster_name else ""
        escaped_table_name = escape_identifier(self._table_name)
        assert escaped_table_name is not None
        return (
            f"ALTER TABLE {escaped_table_name}{on_cluster} MODIFY SETTING "
            "enable_block_number_column = 1, enable_block_offset_column = 1;"
        )

    def execute(self, logger: JobLogger) -> None:
        cluster = get_cluster(StorageSetKey.EVENTS_ANALYTICS_PLATFORM)
        storage_node = cluster.get_local_nodes()[0]
        connection = cluster.get_node_connection(ClickhouseClientSettings.MIGRATE, storage_node)
        cluster_name = None if cluster.is_single_node() else cluster.get_clickhouse_cluster_name()
        query = self._get_query(cluster_name)

        logger.info(f"Executing query: {query}")
        result = connection.execute(query=query)
        logger.info("complete")
        logger.info(repr(result))


class ResetEAPDownsampledTableSettings(SetEAPDownsampledTableSettings):
    """Reset block metadata column settings on a downsampled EAP local table."""

    def _get_query(self, cluster_name: str | None) -> str:
        on_cluster = f" ON CLUSTER '{cluster_name}'" if cluster_name else ""
        escaped_table_name = escape_identifier(self._table_name)
        assert escaped_table_name is not None
        return (
            f"ALTER TABLE {escaped_table_name}{on_cluster} RESET SETTING "
            "enable_block_number_column, enable_block_offset_column;"
        )
