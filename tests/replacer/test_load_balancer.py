from typing import Mapping, MutableSequence, Sequence

import pytest

from snuba.clusters.cluster import ClickhouseNode
from snuba.replacer import RoundRobinConnectionPool
from tests.clusters.fake_cluster import FakeClickhouseCluster

TEST_CASES = [
    pytest.param(
        [
            ClickhouseNode("query_node", 9000, None, None),
            ClickhouseNode("storage-0-0", 9000, 1, 1),
            ClickhouseNode("storage-0-1", 9000, 1, 2),
            ClickhouseNode("storage-1-0", 9000, 2, 1),
            ClickhouseNode("storage-1-1", 9000, 2, 2),
        ],
        1,
        [
            {
                1: [
                    ClickhouseNode("storage-0-0", 9000, 1, 1),
                    ClickhouseNode("storage-0-1", 9000, 1, 2),
                ],
                2: [
                    ClickhouseNode("storage-1-0", 9000, 2, 1),
                    ClickhouseNode("storage-1-1", 9000, 2, 2),
                ],
            }
        ],
        id="1 call to load balancer on 2 shards",
    ),
    pytest.param(
        [
            ClickhouseNode("query_node", 9000, None, None),
            ClickhouseNode("storage-0-0", 9000, 1, 1),
            ClickhouseNode("storage-0-1", 9000, 1, 2),
            ClickhouseNode("storage-1-0", 9000, 2, 1),
            ClickhouseNode("storage-1-1", 9000, 2, 2),
        ],
        2,
        [
            {
                1: [
                    ClickhouseNode("storage-0-0", 9000, 1, 1),
                    ClickhouseNode("storage-0-1", 9000, 1, 2),
                ],
                2: [
                    ClickhouseNode("storage-1-0", 9000, 2, 1),
                    ClickhouseNode("storage-1-1", 9000, 2, 2),
                ],
            },
            {
                1: [
                    ClickhouseNode("storage-0-1", 9000, 1, 2),
                    ClickhouseNode("storage-0-0", 9000, 1, 1),
                ],
                2: [
                    ClickhouseNode("storage-1-1", 9000, 2, 2),
                    ClickhouseNode("storage-1-0", 9000, 2, 1),
                ],
            },
        ],
        id="2 calls to load balancer test wrap around",
    ),
    pytest.param(
        [
            ClickhouseNode("query_node", 9000, None, None),
            ClickhouseNode("storage-0-0", 9000, 1, 1),
            ClickhouseNode("storage-0-1", 9000, 1, 2),
            ClickhouseNode("storage-0-2", 9000, 1, 3),
            ClickhouseNode("storage-1-0", 9000, 2, 1),
        ],
        3,
        [
            {
                1: [
                    ClickhouseNode("storage-0-0", 9000, 1, 1),
                    ClickhouseNode("storage-0-1", 9000, 1, 2),
                    ClickhouseNode("storage-0-2", 9000, 1, 3),
                ],
                2: [ClickhouseNode("storage-1-0", 9000, 2, 1)],
            },
            {
                1: [
                    ClickhouseNode("storage-0-1", 9000, 1, 2),
                    ClickhouseNode("storage-0-2", 9000, 1, 3),
                    ClickhouseNode("storage-0-0", 9000, 1, 1),
                ],
                2: [ClickhouseNode("storage-1-0", 9000, 2, 1)],
            },
            {
                1: [
                    ClickhouseNode("storage-0-2", 9000, 1, 3),
                    ClickhouseNode("storage-0-0", 9000, 1, 1),
                    ClickhouseNode("storage-0-1", 9000, 1, 2),
                ],
                2: [ClickhouseNode("storage-1-0", 9000, 2, 1)],
            },
        ],
        id="Unbalanced cluster",
    ),
    pytest.param(
        [
            ClickhouseNode("query_node", 9000, None, None),
            ClickhouseNode("storage-0-0", 9000, 1, 1),
            ClickhouseNode("storage-0-1", 9000, 1, 2),
            ClickhouseNode("storage-0-2", 9000, 1, 3),
            ClickhouseNode("storage-0-3", 9000, 1, 4),
        ],
        5,
        [
            {
                1: [
                    ClickhouseNode("storage-0-0", 9000, 1, 1),
                    ClickhouseNode("storage-0-1", 9000, 1, 2),
                    ClickhouseNode("storage-0-2", 9000, 1, 3),
                ],
            },
            {
                1: [
                    ClickhouseNode("storage-0-1", 9000, 1, 2),
                    ClickhouseNode("storage-0-2", 9000, 1, 3),
                    ClickhouseNode("storage-0-3", 9000, 1, 4),
                ],
            },
            {
                1: [
                    ClickhouseNode("storage-0-2", 9000, 1, 3),
                    ClickhouseNode("storage-0-3", 9000, 1, 4),
                    ClickhouseNode("storage-0-0", 9000, 1, 1),
                ],
            },
            {
                1: [
                    ClickhouseNode("storage-0-3", 9000, 1, 4),
                    ClickhouseNode("storage-0-0", 9000, 1, 1),
                    ClickhouseNode("storage-0-1", 9000, 1, 2),
                ],
            },
            {
                1: [
                    ClickhouseNode("storage-0-0", 9000, 1, 1),
                    ClickhouseNode("storage-0-1", 9000, 1, 2),
                    ClickhouseNode("storage-0-2", 9000, 1, 3),
                ],
            },
        ],
        id="Full wrap around. Replicaset higher than 3",
    ),
]


@pytest.mark.parametrize("cluster_nodes, iterations, expected_results", TEST_CASES)
def test_load_balancer(
    cluster_nodes: Sequence[ClickhouseNode],
    iterations: int,
    expected_results: Sequence[Mapping[int, Sequence[ClickhouseNode]]],
) -> None:
    cluster = FakeClickhouseCluster(
        host="query_node",
        port=9000,
        user="default",
        password="",
        database="default",
        http_port=8123,
        storage_sets={"events"},
        single_node=False,
        cluster_name="my_cluster",
        distributed_cluster_name="my_distributed_cluster",
        nodes=[(node, True) for node in cluster_nodes],
    )

    load_balancer = RoundRobinConnectionPool(cluster)
    results: MutableSequence[Mapping[int, Sequence[ClickhouseNode]]] = []
    for i in range(iterations):
        results.append(load_balancer.get_connections())

    assert results == expected_results
