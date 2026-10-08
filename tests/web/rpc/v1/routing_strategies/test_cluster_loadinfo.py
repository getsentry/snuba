from collections.abc import Generator
from unittest.mock import Mock, patch

import pytest
from sentry_options.testing import override_options

from snuba.clusters.load_info import LoadInfo, get_cluster_loadinfo


@pytest.fixture(autouse=True)
def enable_get_cluster_loadinfo() -> Generator[None]:
    with override_options("snuba", {"storage_routing.enable_get_cluster_loadinfo": True}):
        yield


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


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_get_cluster_loadinfo_disabled() -> None:
    with override_options("snuba", {"storage_routing.enable_get_cluster_loadinfo": False}):
        assert get_cluster_loadinfo() is None


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


def test_exceeds_true_when_any_field_over_ceiling() -> None:
    load_info = LoadInfo(cluster_load=95.0, concurrent_queries=1.0)
    assert load_info.exceeds({"cluster_load": 90.0}) is True
    # only one field needs to exceed
    assert load_info.exceeds({"cluster_load": 90.0, "concurrent_queries": 100.0}) is True


def test_exceeds_false_when_all_within() -> None:
    load_info = LoadInfo(cluster_load=50.0, concurrent_queries=10.0)
    assert load_info.exceeds({"cluster_load": 90.0, "concurrent_queries": 100.0}) is False


def test_exceeds_empty_thresholds_is_true() -> None:
    # No thresholds configured => keep enforcing, so an idle cluster still "exceeds".
    assert LoadInfo(cluster_load=1.0).exceeds({}) is True


def test_exceeds_true_when_field_negative() -> None:
    # A -1 (missing/unreadable metric, or unknown key) is out of [0, ceiling] =>
    # keep enforcing rather than pardon on blind telemetry.
    assert LoadInfo(cluster_load=-1.0).exceeds({"cluster_load": 90.0}) is True
    assert LoadInfo(cluster_load=1.0).exceeds({"not_a_field": 0.0}) is True
