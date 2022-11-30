from collections import defaultdict
from enum import Enum
from time import sleep
from typing import Any, List, Mapping, Sequence

import structlog
from structlog.contextvars import bind_contextvars, clear_contextvars

from snuba.clickhouse.native import ClickhousePool, ClickhouseResult
from snuba.clusters.cluster import (
    ClickhouseClientSettings,
    ClickhouseCluster,
    ClickhouseNode,
    connection_cache,
)

logger = structlog.get_logger().bind(module=__name__)


class DeleteResult(Enum):
    IN_PROGRESS = 0
    FAILED = 1
    SUCCEEDED = 2


def order_cluster_nodes(
    nodes: Sequence[ClickhouseNode],
) -> Mapping[int, Sequence[ClickhouseNode]]:
    """
    Given a series of unordered ClickHouseNode[s], arrange them by shard
    """
    ordered_nodes: Mapping[int, List[ClickhouseNode]] = defaultdict(list)
    for node in nodes:
        assert (
            node.shard is not None
        ), f"cannot proceed with automatic delete, node {node} has empty shard"
        ordered_nodes[node.shard].append(node)
    return ordered_nodes


def get_working_replica_node(
    nodes: Sequence[ClickhouseNode], user: str, password: str, database: str
) -> ClickhousePool:
    # TODO actually fail to getting another node if we can't load this connection
    return connection_cache.get_node_connection(
        ClickhouseClientSettings.DATA_DELETE, nodes[0], user, password, database
    )


def execute_delete_command_on_replica(
    connection: ClickhousePool,
    query: str,
    params: Mapping[str, Any],
    dry_run: bool = False,
) -> ClickhouseResult:
    logger.info("data_delete.pre_delete", interpolated_query={query % params})
    if dry_run:
        return ClickhouseResult()
    else:
        return connection.execute(query, params, retryable=False)


def assert_no_mutations_on_replica(connection: ClickhousePool) -> None:
    assert not is_mutation_running(connection)


def is_mutation_running(connection: ClickhousePool) -> bool:
    return (
        len(
            connection.execute(
                "SELECT * FROM system.mutations WHERE is_done = 0"
            ).results
        )
        != 0
    )


def data_delete(
    cluster: ClickhouseCluster,
    ch_user: str,
    ch_pass: str,
    query_template: str,
    query_parameters: Sequence[Mapping[str, Any]],
    dry_run: bool = True,
) -> DeleteResult:
    clickhouse_db = cluster.get_database()
    cluster_nodes = order_cluster_nodes(cluster.get_local_nodes())

    for (shard, shard_replicas) in cluster_nodes.items():
        logger.info("data_delete.init", shard_id=shard)
        replica_node = get_working_replica_node(
            shard_replicas, ch_user, ch_pass, clickhouse_db
        )
        for query_parameter_row in query_parameters:
            bind_contextvars(query_parameters=query_parameter_row)
            assert_no_mutations_on_replica(replica_node)
            execute_delete_command_on_replica(
                replica_node, query_template, query_parameter_row, dry_run
            )
            logger.info("data_delete.started")
            while is_mutation_running(replica_node):
                logger.debug("data_delete.sleep")
                sleep(10)
            logger.info("data_delete.finished")
            clear_contextvars()
    return DeleteResult.SUCCEEDED
