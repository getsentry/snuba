from abc import ABC, abstractmethod
from typing import Mapping, Optional, Union

from snuba import settings
from snuba.clickhouse.escaping import escape_string
from snuba.clusters.cluster import ClickhouseCluster
from snuba.clusters.storage_sets import StorageSetKey


class TableEngine(ABC):
    """
    Represents a ClickHouse table engine, such as any table or view.

    Any changes made to these classes should ensure the result of every migration
    does not change.
    """

    @abstractmethod
    def get_sql(self, cluster: ClickhouseCluster, table_name: str) -> str:
        raise NotImplementedError


class MergeTree(TableEngine):
    """
    Provides the SQL for create table operations.

    If the operation is being performed on a cluster marked as multi node, the
    replicated versions of the tables will be created instead of the regular ones.

    The shard and replica values will be taken from the macros configuration for
    multi node clusters, so these need to be set prior to running the migration.

    If unsharded=True is passed, data will be replicated on every shard of the cluster.
    """

    def __init__(
        self,
        storage_set: StorageSetKey,
        order_by: str,
        primary_key: Optional[str] = None,
        partition_by: Optional[str] = None,
        sample_by: Optional[str] = None,
        ttl: Optional[str] = None,
        settings: Optional[Mapping[str, Union[str, int]]] = None,
        unsharded: bool = False,
    ) -> None:
        self._storage_set_value = storage_set.value
        self.__order_by = order_by
        self.__primary_key = primary_key
        self.__partition_by = partition_by
        self.__sample_by = sample_by
        self.__ttl = ttl
        self.__settings = settings
        self._unsharded = unsharded

    def get_sql(self, cluster: ClickhouseCluster, table_name: str) -> str:
        primary_key_sql = ""
        if self.__primary_key:
            primary_key_sql = f" PRIMARY KEY {self.__primary_key}"

        sql = f"{self._get_engine_type(cluster, table_name)}{primary_key_sql} ORDER BY {self.__order_by}"

        if self.__partition_by:
            sql += f" PARTITION BY {self.__partition_by}"

        if self.__sample_by:
            sql += f" SAMPLE BY {self.__sample_by}"

        if self.__ttl:
            sql += f" TTL {self.__ttl}"

        if self.__settings:
            settings = ", ".join([f"{k}={v}" for k, v in self.__settings.items()])
            sql += f" SETTINGS {settings}"

        return sql

    def _get_zookeeper_path(self, cluster: ClickhouseCluster, table_name: str) -> str:
        database_name = cluster.get_database()

        if self._unsharded is True:
            path = f"/clickhouse/tables/{self._storage_set_value}/all/{database_name}/{table_name}"
        else:
            path = f"/clickhouse/tables/{self._storage_set_value}/{{shard}}/{database_name}/{table_name}"

        path_with_override = settings.CLICKHOUSE_ZOOKEEPER_OVERRIDE.get(path, path)

        return f"'{path_with_override}'"

    def _get_engine_type(self, cluster: ClickhouseCluster, table_name: str) -> str:
        if cluster.is_single_node():
            return "MergeTree()"
        else:
            zoo_path = self._get_zookeeper_path(cluster, table_name)
            return f"ReplicatedMergeTree({zoo_path}, '{{replica}}')"


class ReplacingMergeTree(MergeTree):
    def __init__(
        self,
        storage_set: StorageSetKey,
        order_by: str,
        primary_key: Optional[str] = None,
        version_column: Optional[str] = None,
        partition_by: Optional[str] = None,
        sample_by: Optional[str] = None,
        ttl: Optional[str] = None,
        settings: Optional[Mapping[str, str]] = None,
        unsharded: bool = False,
    ) -> None:
        super().__init__(
            storage_set=storage_set,
            order_by=order_by,
            primary_key=primary_key,
            partition_by=partition_by,
            sample_by=sample_by,
            ttl=ttl,
            settings=settings,
            unsharded=unsharded,
        )
        self.__version_column = version_column

    def _get_engine_type(self, cluster: ClickhouseCluster, table_name: str) -> str:
        if cluster.is_single_node():
            if self.__version_column:
                return f"ReplacingMergeTree({self.__version_column})"
            return "ReplacingMergeTree()"
        else:
            zoo_path = self._get_zookeeper_path(cluster, table_name)
            if self.__version_column:
                return f"ReplicatedReplacingMergeTree({zoo_path}, '{{replica}}', {self.__version_column})"
            return f"ReplicatedReplacingMergeTree({zoo_path}, '{{replica}}')"


class SummingMergeTree(MergeTree):
    def _get_engine_type(self, cluster: ClickhouseCluster, table_name: str) -> str:
        if cluster.is_single_node():
            return "SummingMergeTree()"
        else:
            zoo_path = self._get_zookeeper_path(cluster, table_name)
            return f"ReplicatedSummingMergeTree({zoo_path}, '{{replica}}')"


class AggregatingMergeTree(MergeTree):
    def _get_engine_type(self, cluster: ClickhouseCluster, table_name: str) -> str:
        if cluster.is_single_node():
            return "AggregatingMergeTree()"
        else:
            zoo_path = self._get_zookeeper_path(cluster, table_name)
            return f"ReplicatedAggregatingMergeTree({zoo_path}, '{{replica}}')"


class Distributed(TableEngine):
    def __init__(self, local_table_name: str, sharding_key: Optional[str]) -> None:
        self.__local_table_name = local_table_name
        self.__sharding_key = sharding_key

    def get_sql(self, cluster: ClickhouseCluster, table_name: str) -> str:
        cluster_name = cluster.get_clickhouse_cluster_name()
        assert not cluster.is_single_node()
        assert cluster_name is not None
        database_name = cluster.get_database()
        optional_sharding_key = (
            f", {self.__sharding_key}" if self.__sharding_key else ""
        )

        return f"Distributed(`{cluster_name}`, {database_name}, {self.__local_table_name}{optional_sharding_key})"


class Merge(TableEngine):
    def __init__(self, table_name_regex: str) -> None:
        self.__table_name_regex = table_name_regex

    def get_sql(self, cluster: ClickhouseCluster, table_name: str) -> str:
        re = escape_string(self.__table_name_regex)
        return f"Merge(currentDatabase(), {re})"
