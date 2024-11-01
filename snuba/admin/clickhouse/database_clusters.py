import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import List, Sequence

from snuba.admin.clickhouse.common import get_ro_node_connection
from snuba.admin.clickhouse.nodes import get_storage_info
from snuba.clusters.cluster import ClickhouseClientSettings


@dataclass(frozen=True)
class Node:
    cluster: str
    host_name: str
    host_address: str
    port: int
    shard: int
    replica: int
    version: str
    storage_name: str
    is_distributed: bool


@dataclass(frozen=True)
class SystemSetting:
    name: str
    value: str
    default: str
    changed: int
    description: str
    type: str


@dataclass(frozen=True)
class HostInfo:
    host: str
    port: int
    storage_name: str
    is_distributed: bool


# Create a lock for thread-safe access
node_info_lock: threading.Lock = threading.Lock()


def fetch_node_info_from_host(host_info: HostInfo) -> Sequence[Node]:
    with node_info_lock:
        connection = get_ro_node_connection(
            host_info.host,
            host_info.port,
            host_info.storage_name,
            ClickhouseClientSettings.QUERY,
        )

    return [
        Node(
            cluster=result[0],
            host_name=result[1],
            host_address=result[2],
            port=result[3],
            shard=result[4],
            replica=result[5],
            version=result[6],
            storage_name=host_info.storage_name,
            is_distributed=host_info.is_distributed,
        )
        for result in connection.execute(
            "SELECT cluster, host_name, host_address, port, shard_num, replica_num, version() FROM system.clusters WHERE is_local = 1;"
        ).results
    ]


def get_node_info() -> Sequence[Node]:
    node_info: List[Node] = []
    hosts = set()
    for storage_info in get_storage_info():
        for node in storage_info["dist_nodes"]:
            hosts.add(
                HostInfo(
                    node["host"],
                    node["port"],
                    storage_info["storage_name"],
                    True,
                )
            )

        for node in storage_info["local_nodes"]:
            hosts.add(
                HostInfo(
                    node["host"],
                    node["port"],
                    storage_info["storage_name"],
                    False,
                )
            )

    with ThreadPoolExecutor() as executor:
        for result in executor.map(fetch_node_info_from_host, hosts):
            node_info.extend(result)

    return node_info


def get_system_settings(host: str, port: int, storage: str) -> Sequence[SystemSetting]:
    connection = get_ro_node_connection(
        host,
        port,
        storage,
        ClickhouseClientSettings.QUERY,
    )

    return [
        SystemSetting(*result)
        for result in connection.execute(
            "SELECT name, value, default, changed, description, type FROM system.server_settings;"
        ).results
    ]
