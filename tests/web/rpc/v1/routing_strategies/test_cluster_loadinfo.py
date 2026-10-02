from collections.abc import Generator
from unittest.mock import Mock, patch

import pytest
from sentry_options import OptionValue
from sentry_options.testing import override_options

from snuba.clusters.load_info import LoadInfo, get_cluster_loadinfo

# Always present on CH. CGroupUserTimeNormalized / disk_inflight_ops are
# host-dependent and may be -1 without failing the probe.
_REQUIRED_FIELDS = (
    "cluster_load",
    "concurrent_queries",
    "memory_allocated",
    "part_mutation",
)
_ALL_FIELDS = _REQUIRED_FIELDS + (
    "cgroup_user_time_normalized",
    "disk_inflight_ops",
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
    assert load_info.disk_inflight_ops == -1
    assert load_info.memory_allocated == -1
    assert load_info.part_mutation == -1


def test_from_dict_ignores_unknown_keys() -> None:
    load_info = LoadInfo.from_dict({"cluster_load": 2.0, "not_a_field": 99, "also_unknown": None})
    assert load_info.cluster_load == 2.0
    assert load_info.concurrent_queries == -1


ENABLE_LOADINFO: dict[str, OptionValue] = {"storage_routing.enable_get_cluster_loadinfo": True}


@pytest.fixture(autouse=True)
def enable_get_cluster_loadinfo() -> Generator[None]:
    with override_options("snuba", ENABLE_LOADINFO):
        yield


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_get_cluster_loadinfo_disabled() -> None:
    with override_options("snuba", {"storage_routing.enable_get_cluster_loadinfo": False}):
        assert get_cluster_loadinfo() is None
    with override_options("snuba", {"storage_routing.enable_get_cluster_loadinfo": True}):
        assert get_cluster_loadinfo() is not None


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_get_cluster_load() -> None:
    load_info = get_cluster_loadinfo()
    assert load_info is not None
    _assert_probe_ok(load_info)


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_get_cluster_load_from_cache() -> None:
    with patch("time.time") as mock_time:
        mock_time.return_value = 0
        load_info = get_cluster_loadinfo()
        assert load_info is not None

        mock_time.return_value = 59
        second_load_info = get_cluster_loadinfo()
        assert second_load_info is not None
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
        _assert_probe_ok(load_info)


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_get_cluster_load_error_handling() -> None:
    with patch("snuba.clickhouse.connect.ClickhouseConnectPool.execute") as mock_execute:
        mock_execute.side_effect = Exception("Test error")
        load_info = get_cluster_loadinfo()
        assert load_info is not None
        _assert_probe_failed(load_info)
        assert load_info.is_idle() is False


def test_is_idle_reads_dynamic_allocation_policy_flag() -> None:
    load_info = LoadInfo(cluster_load=1.0, concurrent_queries=1)
    assert load_info.is_idle() is False
    with override_options("snuba", {"storage_routing.enable_dynamic_allocation_policy": True}):
        assert load_info.is_idle() is True
