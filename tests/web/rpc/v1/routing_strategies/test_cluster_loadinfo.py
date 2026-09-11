from unittest.mock import Mock, patch

import pytest

from snuba.web.rpc.storage_routing.load_retriever import LoadInfo, get_cluster_loadinfo

# Always present on CH. CGroupUserTimeNormalized / BlockInFlightOps are
# host-dependent and may be -1 without failing the probe.
_REQUIRED_FIELDS = (
    "cluster_load",
    "concurrent_queries",
    "memory_tracking",
    "part_mutation",
)
_ALL_FIELDS = _REQUIRED_FIELDS + (
    "cgroup_user_time_normalized",
    "block_in_flight_ops",
)


def _assert_probe_ok(load_info: LoadInfo) -> None:
    assert load_info is not None
    for field in _REQUIRED_FIELDS:
        assert getattr(load_info, field) != -1, field


def _assert_probe_failed(load_info: LoadInfo) -> None:
    assert load_info is not None
    for field in _ALL_FIELDS:
        assert getattr(load_info, field) == -1, field


def test_from_dict_old_cache_shape() -> None:
    load_info = LoadInfo.from_dict({"cluster_load": 1.5, "concurrent_queries": 3})
    assert load_info.cluster_load == 1.5
    assert load_info.concurrent_queries == 3.0
    assert load_info.cgroup_user_time_normalized == -1
    assert load_info.block_in_flight_ops == -1
    assert load_info.memory_tracking == -1
    assert load_info.part_mutation == -1


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_get_cluster_load() -> None:
    _assert_probe_ok(get_cluster_loadinfo())


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
        _assert_probe_ok(get_cluster_loadinfo())


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_get_cluster_load_error_handling() -> None:
    with patch("snuba.clickhouse.connect.ClickhouseConnectPool.execute") as mock_execute:
        mock_execute.side_effect = Exception("Test error")
        _assert_probe_failed(get_cluster_loadinfo())
