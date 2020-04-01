from snuba import settings

from snuba.clickhouse.native import ClickhousePool, NativeDriverReader
from snuba.clickhouse.query import ClickhouseQuery
from snuba.reader import Reader

class Cluster:
    """
    A cluster is responsible for managing a collection of database nodes.

    Clusters are configurable, and will be instantiated based on user defined settings.

    Each storage must be mapped to a cluster, and this mapping is configurable
    (with the constraint that storages that form part of joined queries need to be
    located on the same cluster).

    A cluster provides a reader and Clickhouse connections that are shared by all
    storages located on the cluster.

    In future, clusters will also be reponsible for co-ordinating commands that
    need to be run on multiple hosts that are colocated within the same cluster -
    e.g. bootstrap, migration, cleanup, optimize.
    """
    def __init__(self, host: str, port: int, http_port: int):
        self.__clickhouse_rw = ClickhousePool(host, port)
        self.__clickhouse_ro = ClickhousePool(
            host,
            port,
            client_settings={"readonly": True},
        )
        self.__reader: Reader[ClickhouseQuery] = NativeDriverReader(self.__clickhouse_ro)

    def get_clickhouse_rw(self) -> ClickhousePool:
        return self.__clickhouse_rw

    def get_clickhouse_ro(self) -> ClickhousePool:
        return self.__clickhouse_ro

    def get_reader(self) -> Reader:
        return self.__reader


cluster = Cluster(
    host=settings.CLICKHOUSE_HOST,
    port=settings.CLICKHOUSE_PORT,
    http_port=settings.CLICKHOUSE_HTTP_PORT,
)

def get_cluster(storage_key: str) -> Cluster:
    """
    Curerently all storages are mapped to a single global cluster.
    In future this will be configurable.
    """
    return cluster
