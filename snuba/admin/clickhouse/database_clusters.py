from dataclasses import dataclass
from typing import Sequence

from snuba.admin.clickhouse.common import get_ro_node_connection
from snuba.admin.clickhouse.nodes import get_storage_info
from snuba.clusters.cluster import ClickhouseClientSettings


@dataclass(frozen=True)
class Node:
    cluster: str
    host_name: str
    host_address: str
    shard: int
    replica: int
    version: str


@dataclass(frozen=True)
class HostInfo:
    host: str
    port: int
    storage_name: str


def get_node_info() -> Sequence[Node]:
    node_info = []
    hosts = set()
    for storage_info in get_storage_info():
        for node in storage_info["dist_nodes"]:
            hosts.add(
                HostInfo(node["host"], node["port"], storage_info["storage_name"])
            )

        for node in storage_info["local_nodes"]:
            hosts.add(
                HostInfo(node["host"], node["port"], storage_info["storage_name"])
            )

    for host_info in hosts:
        connection = get_ro_node_connection(
            host_info.host,
            host_info.port,
            host_info.storage_name,
            ClickhouseClientSettings.QUERY,
        )
        nodes = [
            Node(*result)
            for result in connection.execute(
                "SELECT cluster, host_name, host_address, shard_num, replica_num, version() FROM system.clusters WHERE is_local = 1;"
            ).results
        ]
        node_info.extend(nodes)

    return node_info
