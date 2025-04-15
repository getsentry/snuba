import threading
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

import structlog

from snuba import state
from snuba.admin.clickhouse.common import get_ro_node_connection
from snuba.admin.clickhouse.nodes import get_storage_info
from snuba.clusters.cluster import ClickhouseClientSettings

logger = structlog.get_logger(__name__)


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

# Circuit breaker pattern to avoid repeatedly trying down nodes
# Map of "host:port" -> timestamp when to retry
_circuit_breaker: Dict[str, float] = {}
# How long to wait before retrying a failed node (in seconds)
CIRCUIT_BREAKER_TIMEOUT = 60


def should_skip_node(host: str, port: int) -> bool:
    """
    Check if a node should be skipped based on circuit breaker pattern
    """
    key = f"{host}:{port}"
    with node_info_lock:  # Use the same lock for thread safety
        if key in _circuit_breaker:
            if time.time() < _circuit_breaker[key]:
                return True
            else:
                # Reset circuit breaker
                del _circuit_breaker[key]
    return False


def mark_node_failed(host: str, port: int) -> None:
    """
    Mark a node as failed in the circuit breaker
    """
    key = f"{host}:{port}"
    with node_info_lock:
        _circuit_breaker[key] = time.time() + CIRCUIT_BREAKER_TIMEOUT


def fetch_node_info_from_host(host_info: HostInfo) -> Sequence[Node]:
    # Check if the node should be skipped due to previous failures
    if should_skip_node(host_info.host, host_info.port):
        logger.info(
            "Skipping node - circuit open due to previous failures",
            host=host_info.host,
            port=host_info.port,
            storage=host_info.storage_name,
        )
        return []

    try:
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
    except Exception as exc:
        # Mark the node as failed in the circuit breaker
        mark_node_failed(host_info.host, host_info.port)
        logger.warning(
            "Failed to fetch node info",
            exc_info=True,
            host=host_info.host,
            port=host_info.port,
            storage=host_info.storage_name,
        )
        return []


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
        # Submit all tasks and get futures
        futures = {
            executor.submit(fetch_node_info_from_host, host_info): host_info
            for host_info in hosts
        }

        # Process futures as they complete with a timeout
        for future in list(futures.keys()):
            host_info = futures[future]
            try:
                result = future.result(
                    timeout=2
                )  # Set a 2-second timeout for each node
                node_info.extend(result)
            except FuturesTimeoutError:
                # Mark the node as failed in the circuit breaker
                mark_node_failed(host_info.host, host_info.port)
                logger.warning(
                    "Timeout while fetching node info",
                    host=host_info.host,
                    port=host_info.port,
                    storage=host_info.storage_name,
                )
            except Exception as exc:
                # This should be caught by fetch_node_info_from_host, but add an extra safeguard
                logger.warning(
                    "Error processing node info result",
                    exc_info=True,
                    host=host_info.host,
                    port=host_info.port,
                    storage=host_info.storage_name,
                )

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
