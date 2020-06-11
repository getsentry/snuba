from abc import ABC, abstractmethod

from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey


class Operation(ABC):
    """
    Executed on all the nodes of the cluster.
    """

    @abstractmethod
    def execute(self) -> None:
        raise NotImplementedError


class SqlOperation(Operation, ABC):
    def __init__(self, storage_set: StorageSetKey):
        self._storage_set = storage_set

    def execute(self) -> None:
        cluster = get_cluster(self._storage_set)

        for node in cluster.get_local_nodes():
            connection = cluster.get_node_connection(
                ClickhouseClientSettings.MIGRATE, node
            )
            connection.execute(self.format_sql())

    @abstractmethod
    def format_sql(self) -> str:
        raise NotImplementedError


class RunSql(SqlOperation):
    def __init__(self, storage_set: StorageSetKey, statement: str) -> None:
        self.statement = statement
        super().__init__(storage_set)

    def format_sql(self) -> str:
        return self.statement


class DropTable(SqlOperation):
    def __init__(self, storage_set: StorageSetKey, table_name: str) -> None:
        self.table_name = table_name
        super().__init__(storage_set)

    def format_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self.table_name};"
