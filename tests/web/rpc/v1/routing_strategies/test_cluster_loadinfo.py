from unittest.mock import Mock, patch

import pytest

from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.load_retriever import (
    get_cluster_loadinfo,
)


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_get_cluster_load() -> None:
    load_info = get_cluster_loadinfo()
    assert load_info is not None
    assert load_info.cluster_load != -1.0
    assert load_info.concurrent_queries != -1


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_get_cluster_load_from_cache() -> None:
    with patch("time.time") as mock_time:
        mock_time.return_value = 0
        load_info = get_cluster_loadinfo()

        mock_time.return_value = 59
        second_load_info = get_cluster_loadinfo()
        assert load_info.to_dict() == second_load_info.to_dict()


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_get_cluster_loadinfo_if_cache_fails() -> None:
    mock_redis = Mock()
    mock_redis.side_effect = Exception("Test error")
    with patch("snuba.redis.get_redis_client") as mock_redis_client:
        mock_redis_client.return_value = mock_redis
        load_info = get_cluster_loadinfo()
        assert load_info is not None
        assert load_info.cluster_load != -1.0
        assert load_info.concurrent_queries != -1


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_get_cluster_load_error_handling() -> None:
    with patch("snuba.clusters.cluster.ClickhousePool.execute") as mock_execute:
        mock_execute.side_effect = Exception("Test error")
        load_info = get_cluster_loadinfo()
        assert load_info is not None
        assert load_info.cluster_load == -1.0
        assert load_info.concurrent_queries == -1
