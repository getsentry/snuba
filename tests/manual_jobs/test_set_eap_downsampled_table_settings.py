from unittest.mock import Mock

import pytest

from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.manual_jobs import JobSpec
from snuba.manual_jobs.set_eap_downsampled_table_settings import (
    ResetEAPDownsampledTableSettings,
    SetEAPDownsampledTableSettings,
)


def _build_job(monkeypatch: pytest.MonkeyPatch, table_name: str) -> SetEAPDownsampledTableSettings:
    monkeypatch.setattr("snuba.manual_jobs._set_job_type", Mock())
    return SetEAPDownsampledTableSettings(
        JobSpec(
            job_id="set-eap-settings",
            job_type="SetEAPDownsampledTableSettings",
            params={"table_name": table_name},
        )
    )


def test_jobs_allow_adhoc_run() -> None:
    assert SetEAPDownsampledTableSettings.allow_adhoc_run
    assert ResetEAPDownsampledTableSettings.allow_adhoc_run


def test_get_query(monkeypatch: pytest.MonkeyPatch) -> None:
    job = _build_job(monkeypatch, "eap_items_1_downsample_8_local")

    assert job._get_query("eap_cluster") == (
        "ALTER TABLE eap_items_1_downsample_8_local ON CLUSTER 'eap_cluster' MODIFY SETTING "
        "enable_block_number_column = 1, enable_block_offset_column = 1;"
    )


def test_get_reset_query(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("snuba.manual_jobs._set_job_type", Mock())
    job = ResetEAPDownsampledTableSettings(
        JobSpec(
            job_id="reset-eap-settings",
            job_type="ResetEAPDownsampledTableSettings",
            params={"table_name": "eap_items_1_downsample_8_local"},
        )
    )

    assert job._get_query("eap_cluster") == (
        "ALTER TABLE eap_items_1_downsample_8_local ON CLUSTER 'eap_cluster' RESET SETTING "
        "enable_block_number_column, enable_block_offset_column;"
    )


@pytest.mark.parametrize(
    "table_name",
    [
        "eap_items_1_downsample_8_local",
        "eap_items_1_downsample_64_local",
        "eap_items_1_downsample_512_local",
    ],
)
def test_accepts_supported_downsampled_eap_local_tables(
    monkeypatch: pytest.MonkeyPatch, table_name: str
) -> None:
    _build_job(monkeypatch, table_name)


@pytest.mark.parametrize(
    "table_name",
    [
        "eap_items_1_local",
        "eap_items_1_downsample_8_dist",
        "eap_items_1_downsample_16_local",
        "eap_items_1_downsample_8_local; DROP TABLE events",
        "events_local",
    ],
)
def test_rejects_non_downsampled_eap_local_table(
    monkeypatch: pytest.MonkeyPatch, table_name: str
) -> None:
    with pytest.raises(AssertionError, match="downsampled EAP local table"):
        _build_job(monkeypatch, table_name)


def test_execute_uses_eap_local_cluster(monkeypatch: pytest.MonkeyPatch) -> None:
    job = _build_job(monkeypatch, "eap_items_1_downsample_64_local")
    connection = Mock()
    cluster = Mock()
    cluster.get_local_nodes.return_value = ["local-node"]
    cluster.is_single_node.return_value = False
    cluster.get_clickhouse_cluster_name.return_value = "eap_cluster"
    cluster.get_node_connection.return_value = connection
    monkeypatch.setattr(
        "snuba.manual_jobs.set_eap_downsampled_table_settings.get_cluster",
        Mock(return_value=cluster),
    )
    logger = Mock()

    job.execute(logger)

    cluster.get_node_connection.assert_called_once_with(
        ClickhouseClientSettings.MIGRATE, "local-node"
    )
    connection.execute.assert_called_once_with(
        query=(
            "ALTER TABLE eap_items_1_downsample_64_local ON CLUSTER 'eap_cluster' "
            "MODIFY SETTING enable_block_number_column = 1, "
            "enable_block_offset_column = 1;"
        )
    )


def test_reset_execute_uses_eap_local_cluster(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("snuba.manual_jobs._set_job_type", Mock())
    job = ResetEAPDownsampledTableSettings(
        JobSpec(
            job_id="reset-eap-settings",
            job_type="ResetEAPDownsampledTableSettings",
            params={"table_name": "eap_items_1_downsample_64_local"},
        )
    )
    connection = Mock()
    cluster = Mock()
    cluster.get_local_nodes.return_value = ["local-node"]
    cluster.is_single_node.return_value = False
    cluster.get_clickhouse_cluster_name.return_value = "eap_cluster"
    cluster.get_node_connection.return_value = connection
    monkeypatch.setattr(
        "snuba.manual_jobs.set_eap_downsampled_table_settings.get_cluster",
        Mock(return_value=cluster),
    )

    job.execute(Mock())

    connection.execute.assert_called_once_with(
        query=(
            "ALTER TABLE eap_items_1_downsample_64_local ON CLUSTER 'eap_cluster' "
            "RESET SETTING enable_block_number_column, enable_block_offset_column;"
        )
    )
