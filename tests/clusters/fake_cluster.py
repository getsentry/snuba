from typing import Any, List, Mapping, MutableMapping, Optional, Sequence, Set, Tuple

from snuba.clickhouse.native import ClickhousePool, Params
from snuba.clusters.cluster import (
    ClickhouseClientSettings,
    ClickhouseCluster,
    ClickhouseNode,
)


class FakeClickhousePool(ClickhousePool):
    def __init__(self) -> None:
        self.__queries: List[str] = []

    def execute(
        self,
        query: str,
        params: Params = None,
        with_column_types: bool = False,
        query_id: Optional[str] = None,
        settings: Optional[Mapping[str, Any]] = None,
        types_check: bool = False,
        columnar: bool = False,
    ) -> Sequence[Any]:
        self.__queries.append(query)
        return [[1]]

    def get_queries(self) -> Sequence[str]:
        return self.__queries


class FakeClickhouseCluster(ClickhouseCluster):
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        http_port: int,
        storage_sets: Set[str],
        single_node: bool,
        # The cluster name and distributed cluster name only apply if single_node is set to False
        cluster_name: Optional[str] = None,
        distributed_cluster_name: Optional[str] = None,
        nodes: Optional[Sequence[ClickhouseNode]] = None,
    ):
        super().__init__(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            http_port=http_port,
            storage_sets=storage_sets,
            single_node=single_node,
            cluster_name=cluster_name,
            distributed_cluster_name=distributed_cluster_name,
        )
        self.__distributed_cluster_name = distributed_cluster_name
        self.__cluster_name = cluster_name
        self.__nodes = nodes or []
        self.__connections: MutableMapping[
            Tuple[ClickhouseNode, ClickhouseClientSettings], FakeClickhousePool
        ] = {}

    def get_queries(
        self,
    ) -> Mapping[Tuple[ClickhouseNode, ClickhouseClientSettings], Sequence[str]]:
        return {
            key: self.__connections[key].get_queries() for key in self.__connections
        }

    def clean_connections(self) -> None:
        self.__connections = {}

    def get_local_nodes(self) -> Sequence[ClickhouseNode]:
        if self.is_single_node():
            return [self.__query_node]

        assert self.__cluster_name is not None, "cluster_name must be set"
        return self.__nodes

    def get_distributed_nodes(self) -> Sequence[ClickhouseNode]:
        if self.is_single_node():
            return []
        assert (
            self.__distributed_cluster_name is not None
        ), "distributed_cluster_name must be set"
        return self.__nodes

    def get_node_connection(
        self, client_settings: ClickhouseClientSettings, node: ClickhouseNode,
    ) -> ClickhousePool:
        settings, timeout = client_settings.value
        cache_key = (node, client_settings)
        if cache_key not in self.__connections:
            self.__connections[cache_key] = FakeClickhousePool()
        return self.__connections[cache_key]
