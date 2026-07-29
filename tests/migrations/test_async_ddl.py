from __future__ import annotations

from typing import Any
from unittest.mock import Mock, patch

import pytest

from snuba.clusters.cluster import ClickhouseCluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.async_ddl import (
    DistributedDDLError,
    DistributedDDLTimeout,
    execute_ddl_async,
)
from snuba.migrations.operations import OperationTarget, RunSql

FINISHED = "Finished"
INACTIVE = "Inactive"


def _result(rows: list[tuple[Any, ...]]) -> Mock:
    result = Mock()
    result.results = rows
    return result


def _pool(poll_results: list[list[tuple[Any, ...]]]) -> Mock:
    """A connection whose first execute() is the submit, then one per poll."""
    pool = Mock()
    pool.execute.side_effect = [_result([])] + [_result(rows) for rows in poll_results]
    return pool


def test_submits_without_waiting_and_tags_the_task() -> None:
    pool = _pool([[("query-1", "n1", FINISHED, 0, "")]])

    execute_ddl_async(
        pool,
        "CREATE TABLE t ON CLUSTER 'c1' (a UInt64) ENGINE MergeTree ORDER BY a",
        cluster_name="c1",
    )

    submit_settings = pool.execute.call_args_list[0].kwargs["settings"]
    # task_timeout=0 is what makes the submission return immediately, which is the
    # whole point: waiting inline is what produces truncated "IncompleteRead" bodies.
    assert submit_settings["distributed_ddl_task_timeout"] == 0
    assert submit_settings["distributed_ddl_output_mode"] == "never_throw"
    assert submit_settings["log_comment"].startswith("snuba-migration-")

    # The poll query correlates on that same log_comment.
    poll_query = pool.execute.call_args_list[1].args[0]
    assert "system.distributed_ddl_queue" in poll_query
    assert submit_settings["log_comment"] in poll_query


def test_preserves_caller_settings() -> None:
    pool = _pool([[("query-1", "n1", FINISHED, 0, "")]])

    execute_ddl_async(
        pool,
        "ALTER TABLE t ON CLUSTER 'c1' DROP INDEX i",
        cluster_name="c1",
        query_settings={"allow_suspicious_primary_key": 1},
    )

    submit_settings = pool.execute.call_args_list[0].kwargs["settings"]
    assert submit_settings["allow_suspicious_primary_key"] == 1
    assert submit_settings["distributed_ddl_task_timeout"] == 0


def test_succeeds_when_all_hosts_finish() -> None:
    pool = _pool(
        [
            [("query-1", "n1", FINISHED, 0, ""), ("query-1", "n2", INACTIVE, None, None)],
            [("query-1", "n1", FINISHED, 0, ""), ("query-1", "n2", FINISHED, 0, "")],
        ]
    )

    execute_ddl_async(
        pool,
        "CREATE TABLE t ON CLUSTER 'c1' (a UInt64) ENGINE Log",
        cluster_name="c1",
        poll_interval_seconds=0,
    )

    # submit + 2 polls
    assert pool.execute.call_count == 3


def test_raises_real_error_from_queue() -> None:
    """A failing DDL must surface the server's message, not an opaque transport error."""
    pool = _pool(
        [
            [
                ("query-1", "n1", FINISHED, 16, "Code: 16. Wrong column name."),
                ("query-1", "n2", FINISHED, 16, "Code: 16. Wrong column name."),
            ]
        ]
    )

    with pytest.raises(DistributedDDLError) as excinfo:
        execute_ddl_async(
            pool,
            "ALTER TABLE t ON CLUSTER 'c1' ADD COLUMN b UInt64 AFTER nope",
            cluster_name="c1",
            poll_interval_seconds=0,
        )

    message = str(excinfo.value)
    assert "Wrong column name" in message
    assert "n1" in message and "n2" in message


def test_timeout_names_the_unfinished_host() -> None:
    """This is the case that used to be IncompleteRead(N bytes read)."""
    pool = Mock()
    pool.execute.side_effect = [_result([])] + [
        _result([("query-1", "n1", FINISHED, 0, ""), ("query-1", "n2", INACTIVE, None, None)])
        for _ in range(50)
    ]

    with pytest.raises(DistributedDDLTimeout) as excinfo:
        execute_ddl_async(
            pool,
            "CREATE TABLE t ON CLUSTER 'c1' (a UInt64) ENGINE Log",
            cluster_name="c1",
            timeout_seconds=0,
            poll_interval_seconds=0,
        )

    message = str(excinfo.value)
    assert "n2 (Inactive)" in message
    assert "n1" not in message.split("Unfinished hosts:")[1].split(".")[0]
    # Must tell the operator the task is still pending and the re-run is safe.
    assert "still queued" in message
    assert "system.distributed_ddl_queue" in message


def test_timeout_when_task_never_appears() -> None:
    pool = Mock()
    pool.execute.side_effect = [_result([])] + [_result([]) for _ in range(50)]

    with pytest.raises(DistributedDDLTimeout) as excinfo:
        execute_ddl_async(
            pool,
            "CREATE TABLE t ON CLUSTER 'c1' (a UInt64) ENGINE Log",
            cluster_name="c1",
            timeout_seconds=0,
            poll_interval_seconds=0,
        )

    assert "task not visible in queue" in str(excinfo.value)


def _mock_cluster(single_node: bool) -> Mock:
    cluster = Mock(spec=ClickhouseCluster)
    cluster.is_single_node.return_value = single_node
    cluster.get_clickhouse_cluster_name.return_value = None if single_node else "test_cluster"
    cluster.get_clickhouse_distributed_cluster_name.return_value = (
        None if single_node else "test_cluster"
    )
    cluster.get_local_nodes.return_value = [Mock()]
    cluster.get_distributed_nodes.return_value = [Mock()]
    return cluster


class TestOperationIntegration:
    """SqlOperation.execute() should route ON CLUSTER DDL through the async path."""

    @patch("snuba.migrations.operations.execute_ddl_async")
    @patch("snuba.migrations.operations.get_cluster")
    def test_multi_node_uses_async(self, mock_get_cluster: Mock, mock_async: Mock) -> None:
        mock_get_cluster.return_value = _mock_cluster(single_node=False)

        RunSql(
            StorageSetKey.EVENTS,
            "ALTER TABLE t ON CLUSTER 'test_cluster' MODIFY COLUMN a UInt64",
            target=OperationTarget.LOCAL,
        ).execute()

        assert mock_async.call_count == 1
        assert mock_async.call_args.kwargs["cluster_name"] == "test_cluster"

    @patch("snuba.migrations.operations.execute_ddl_async")
    @patch("snuba.migrations.operations.get_cluster")
    def test_single_node_stays_synchronous(self, mock_get_cluster: Mock, mock_async: Mock) -> None:
        cluster = _mock_cluster(single_node=True)
        mock_get_cluster.return_value = cluster

        RunSql(
            StorageSetKey.EVENTS,
            "ALTER TABLE t MODIFY COLUMN a UInt64",
            target=OperationTarget.LOCAL,
        ).execute()

        # No ON CLUSTER to coordinate, so there is nothing to poll for.
        assert mock_async.call_count == 0
        assert cluster.get_node_connection.return_value.execute.call_count == 1

    @patch("snuba.migrations.operations.settings")
    @patch("snuba.migrations.operations.execute_ddl_async")
    @patch("snuba.migrations.operations.get_cluster")
    def test_async_can_be_disabled(
        self, mock_get_cluster: Mock, mock_async: Mock, mock_settings: Mock
    ) -> None:
        mock_settings.ASYNC_MIGRATION_DDL = False
        mock_settings.LOG_MIGRATIONS = False
        cluster = _mock_cluster(single_node=False)
        mock_get_cluster.return_value = cluster

        RunSql(
            StorageSetKey.EVENTS,
            "ALTER TABLE t ON CLUSTER 'test_cluster' MODIFY COLUMN a UInt64",
            target=OperationTarget.LOCAL,
        ).execute()

        assert mock_async.call_count == 0
        assert cluster.get_node_connection.return_value.execute.call_count == 1
