from typing import Mapping, Sequence

import pytest

from snuba.clusters.cluster import ClickhouseNode
from snuba.replacer import InOrderConnectionPool
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
        {
            1: [
                ClickhouseNode("storage-0-0", 9000, 1, 1),
                ClickhouseNode("storage-0-1", 9000, 1, 2),
                ClickhouseNode("storage-0-2", 9000, 1, 3),
            ],
            2: [ClickhouseNode("storage-1-0", 9000, 2, 1)],
        },
        id="Unbalanced cluster",
    ),
    pytest.param(
        [
            ClickhouseNode("query_node", 9000, None, None),
            ClickhouseNode("storage-1-1", 9000, 2, 2),
            ClickhouseNode("storage-0-1", 9000, 1, 2),
            ClickhouseNode("storage-0-0", 9000, 1, 1),
            ClickhouseNode("storage-1-0", 9000, 2, 1),
        ],
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
        id="Out of order nodes",
    ),
]


@pytest.mark.parametrize(
    "cluster_nodes, expected_inorder_results",
    TEST_CASES,
)
def test_load_balancer(
    cluster_nodes: Sequence[ClickhouseNode],
    expected_inorder_results: Mapping[int, Sequence[ClickhouseNode]],
) -> None:
    cluster = FakeClickhouseCluster(
        host="query_node",
        port=9000,
        user="default",
        password="",
        database="default",
        http_port=8123,
        secure=True,
        ca_certs=None,
        verify=True,
        storage_sets={"events"},
        single_node=False,
        cluster_name="my_cluster",
        distributed_cluster_name="my_distributed_cluster",
        nodes=[(node, True) for node in cluster_nodes],
    )

    in_order_load_balancer = InOrderConnectionPool(cluster)
    in_order_results = in_order_load_balancer.get_connections()

    assert in_order_results == expected_inorder_results
