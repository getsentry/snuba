from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterator
from unittest.mock import Mock, patch

import pytest
import rapidjson
from arroyo.backends.kafka import KafkaPayload
from arroyo.types import BrokerValue, Message, Partition, Topic

from snuba import state
from snuba.clusters.cluster import ClickhouseNode
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.lw_deletions.batching import BatchStepCustom
from snuba.lw_deletions.formatters import SearchIssuesFormatter
from snuba.lw_deletions.strategy import FormatQuery, increment_by
from snuba.lw_deletions.types import ConditionsType
from snuba.redis import RedisClientKey, get_redis_client
from snuba.utils.streams.topics import Topic as SnubaTopic
from snuba.web.bulk_delete_query import DeleteQueryMessage
from snuba.web.delete_query import TooManyOngoingMutationsError

ROWS_CONDITIONS = {
    5: {"project_id": [1], "group_id": [1, 2, 3, 4]},
    6: {"project_id": [2], "group_id": [1, 2, 3, 4]},
    1: {"project_id": [2], "group_id": [4, 5, 6, 7]},
    8: {"project_id": [2], "group_id": [8, 9]},
}


def _get_message(rows: int, conditions: ConditionsType) -> DeleteQueryMessage:
    return {
        "rows_to_delete": rows,
        "storage_name": "search_issues",
        "conditions": conditions,
        "tenant_ids": {"project_id": 1, "organization_id": 1},
    }


def generate_message() -> Iterator[Message[KafkaPayload]]:
    epoch = datetime(1970, 1, 1)
    i = 0
    messages = [
        rapidjson.dumps(_get_message(rows_to_delete, conditions)).encode("utf-8")
        for rows_to_delete, conditions in ROWS_CONDITIONS.items()
    ]

    while True:
        yield Message(
            BrokerValue(
                KafkaPayload(None, messages[i], []),
                Partition(Topic(SnubaTopic.LW_DELETIONS_GENERIC_EVENTS.value), 0),
                i,
                epoch,
            )
        )
        i += 1


@patch("snuba.lw_deletions.strategy._num_ongoing_mutations", return_value=1)
@patch("snuba.lw_deletions.strategy._execute_query")
@pytest.mark.redis_db
def test_multiple_batches_strategies(mock_execute: Mock, mock_num_mutations: Mock) -> None:
    commit_step = Mock()
    metrics = Mock()
    storage = get_writable_storage(StorageKey("search_issues"))

    strategy = BatchStepCustom(
        max_batch_size=8,
        max_batch_time=1000,
        next_step=FormatQuery(commit_step, storage, SearchIssuesFormatter(), metrics),
        increment_by=increment_by,
    )
    make_message = generate_message()
    strategy.submit(next(make_message))
    strategy.submit(next(make_message))
    strategy.submit(next(make_message))
    strategy.submit(next(make_message))
    strategy.close()
    strategy.join()

    assert mock_execute.call_count == 2
    assert commit_step.submit.call_count == 2


@patch("snuba.lw_deletions.strategy._num_ongoing_mutations", return_value=1)
@patch("snuba.lw_deletions.strategy._execute_query")
@pytest.mark.redis_db
def test_clickhouse_settings(mock_execute: Mock, mock_num_mutations: Mock) -> None:
    commit_step = Mock()
    metrics = Mock()
    storage = get_writable_storage(StorageKey("search_issues"))

    strategy = BatchStepCustom(
        max_batch_size=8,
        max_batch_time=1000,
        next_step=FormatQuery(commit_step, storage, SearchIssuesFormatter(), metrics),
        increment_by=increment_by,
    )
    state.set_config("lightweight_deletes_sync", 2)
    make_message = generate_message()
    strategy.submit(next(make_message))
    strategy.submit(next(make_message))
    strategy.submit(next(make_message))
    # use different setting for second execute_query
    state.set_config("lightweight_deletes_sync", 0)
    strategy.submit(next(make_message))
    strategy.close()
    strategy.join()

    assert mock_execute.call_count == 2
    assert commit_step.submit.call_count == 2

    clickhouse_settings = mock_execute.call_args_list[0][1][
        "query_settings"
    ].get_clickhouse_settings()
    assert clickhouse_settings["lightweight_deletes_sync"] == 2
    clickhouse_settings = mock_execute.call_args_list[1][1][
        "query_settings"
    ].get_clickhouse_settings()
    assert clickhouse_settings["lightweight_deletes_sync"] == 0

    assert commit_step.submit.call_count == 2


@patch("snuba.lw_deletions.strategy._num_ongoing_mutations", return_value=1)
@patch("snuba.lw_deletions.strategy._execute_query")
@pytest.mark.redis_db
def test_single_batch(mock_execute: Mock, mock_num_mutations: Mock) -> None:
    commit_step = Mock()
    metrics = Mock()
    storage = get_writable_storage(StorageKey("search_issues"))

    strategy = BatchStepCustom(
        max_batch_size=8,
        max_batch_time=1000,
        next_step=FormatQuery(commit_step, storage, SearchIssuesFormatter(), metrics),
        increment_by=increment_by,
    )
    message = Message(
        BrokerValue(
            KafkaPayload(
                None,
                rapidjson.dumps(_get_message(10, {"project_id": [1], "group_id": [1]})).encode(
                    "utf-8"
                ),
                [],
            ),
            Partition(Topic(SnubaTopic.LW_DELETIONS_GENERIC_EVENTS.value), 0),
            0,
            datetime(1970, 1, 1),
        )
    )
    strategy.submit(message)
    strategy.join(2.0)

    assert mock_execute.call_count == 1
    assert commit_step.submit.call_count == 1


@patch("snuba.lw_deletions.strategy._num_ongoing_mutations", return_value=10)
@patch("snuba.lw_deletions.strategy._execute_query")
@pytest.mark.redis_db
def test_too_many_mutations(mock_execute: Mock, mock_num_mutations: Mock) -> None:
    """
    Before we execute the DELETE FROM query, we check to see how many
    ongoing mutations there are.If there are more ongoing mutations than
    the max allows, we raise MessageRejected and back pressure is applied.

    The max is 5 (the default) and our mocked ongoing mutations is 10.
    """
    commit_step = Mock()
    metrics = Mock()
    storage = get_writable_storage(StorageKey("search_issues"))

    strategy = BatchStepCustom(
        max_batch_size=8,
        max_batch_time=1000,
        next_step=FormatQuery(commit_step, storage, SearchIssuesFormatter(), metrics),
        increment_by=increment_by,
    )
    message = Message(
        BrokerValue(
            KafkaPayload(
                None,
                rapidjson.dumps(_get_message(10, {"project_id": [2], "group_id": [2]})).encode(
                    "utf-8"
                ),
                [],
            ),
            Partition(Topic(SnubaTopic.LW_DELETIONS_GENERIC_EVENTS.value), 0),
            1,
            datetime(1970, 1, 1),
        )
    )
    strategy.submit(message)
    strategy.join(2.0)

    assert mock_execute.call_count == 0
    assert commit_step.submit.call_count == 0


def _make_single_message(
    rows: int = 10, conditions: ConditionsType | None = None
) -> Message[KafkaPayload]:
    if conditions is None:
        conditions = {"project_id": [1], "group_id": [1]}
    return Message(
        BrokerValue(
            KafkaPayload(
                None,
                rapidjson.dumps(_get_message(rows, conditions)).encode("utf-8"),
                [],
            ),
            Partition(Topic(SnubaTopic.LW_DELETIONS_GENERIC_EVENTS.value), 0),
            0,
            datetime(1970, 1, 1),
        )
    )


@patch("snuba.lw_deletions.strategy._num_ongoing_mutations", return_value=1)
@patch("snuba.lw_deletions.strategy._execute_query")
@patch.object(
    FormatQuery,
    "_FormatQuery__partition_column",
    new="receive_timestamp",
    create=True,
)
@pytest.mark.redis_db
def test_split_by_partition_enabled(mock_execute: Mock, mock_num_mutations: Mock) -> None:
    """
    When partition splitting is enabled and system.parts returns 3 Monday dates,
    _execute_query should be called 3 times (once per partition) instead of 1.
    """
    commit_step = Mock()
    metrics = Mock()
    storage = get_writable_storage(StorageKey("search_issues"))

    state.set_config("lw_deletes_split_by_partition_search_issues", 1)

    format_query = FormatQuery(commit_step, storage, SearchIssuesFormatter(), metrics)

    with (
        patch.object(
            format_query,
            "_FormatQuery__partition_column",
            "receive_timestamp",
        ),
        patch.object(
            FormatQuery,
            "_get_partition_dates",
            return_value=["2024-01-15", "2024-01-22", "2024-01-29"],
        ),
    ):
        strategy = BatchStepCustom(
            max_batch_size=8,
            max_batch_time=1000,
            next_step=format_query,
            increment_by=increment_by,
        )
        strategy.submit(_make_single_message())
        strategy.join(2.0)

    # 1 table * 3 partitions = 3 calls
    assert mock_execute.call_count == 3
    assert commit_step.submit.call_count == 1

    # Verify partition_delete_executed metric emitted for each partition
    increment_calls = [
        c for c in metrics.increment.call_args_list if c[0][0] == "partition_delete_executed"
    ]
    assert len(increment_calls) == 3


@patch("snuba.lw_deletions.strategy._num_ongoing_mutations", return_value=1)
@patch("snuba.lw_deletions.strategy._execute_query")
@pytest.mark.redis_db
def test_split_by_partition_disabled(mock_execute: Mock, mock_num_mutations: Mock) -> None:
    """
    When partition splitting config is disabled (default), the original behavior
    should be preserved: 1 call per table.
    """
    commit_step = Mock()
    metrics = Mock()
    storage = get_writable_storage(StorageKey("search_issues"))

    # Ensure config is off (default)
    state.set_config("lw_deletes_split_by_partition_search_issues", 0)

    strategy = BatchStepCustom(
        max_batch_size=8,
        max_batch_time=1000,
        next_step=FormatQuery(commit_step, storage, SearchIssuesFormatter(), metrics),
        increment_by=increment_by,
    )
    strategy.submit(_make_single_message())
    strategy.join(2.0)

    # 1 table * 1 un-split call = 1
    assert mock_execute.call_count == 1
    assert commit_step.submit.call_count == 1


@patch("snuba.lw_deletions.strategy._num_ongoing_mutations", return_value=1)
@patch("snuba.lw_deletions.strategy._execute_query")
@pytest.mark.redis_db
def test_split_by_partition_redis_tracking(mock_execute: Mock, mock_num_mutations: Mock) -> None:
    """
    Issue a batch with partition splitting enabled. Verify Redis SET is populated.
    Re-submit the same batch and verify _execute_query is NOT called again
    (partitions skipped via Redis tracking).
    """
    commit_step = Mock()
    metrics = Mock()
    storage = get_writable_storage(StorageKey("search_issues"))

    state.set_config("lw_deletes_split_by_partition_search_issues", 1)

    partition_dates = ["2024-01-15", "2024-01-22"]

    format_query = FormatQuery(commit_step, storage, SearchIssuesFormatter(), metrics)

    with (
        patch.object(
            format_query,
            "_FormatQuery__partition_column",
            "receive_timestamp",
        ),
        patch.object(
            FormatQuery,
            "_get_partition_dates",
            return_value=partition_dates,
        ),
    ):
        strategy = BatchStepCustom(
            max_batch_size=8,
            max_batch_time=1000,
            next_step=format_query,
            increment_by=increment_by,
        )
        strategy.submit(_make_single_message())
        strategy.join(2.0)

    # First submission: 1 table * 2 partitions = 2 calls
    assert mock_execute.call_count == 2
    assert commit_step.submit.call_count == 1

    # Verify Redis SET is populated
    redis_client = get_redis_client(RedisClientKey.CONFIG)
    # Find the tracking key
    keys = list(redis_client.scan_iter("lw_delete_partitions:search_issues:*"))
    assert len(keys) == 1
    tracking_key = keys[0]
    members = redis_client.smembers(tracking_key)
    expected_members = {
        b"search_issues_local_v2:2024-01-15",
        b"search_issues_local_v2:2024-01-22",
    }
    assert members == expected_members

    # Reset mocks for second submission
    mock_execute.reset_mock()
    commit_step.reset_mock()

    # Create new format_query for the second submission
    format_query2 = FormatQuery(commit_step, storage, SearchIssuesFormatter(), metrics)

    with (
        patch.object(
            format_query2,
            "_FormatQuery__partition_column",
            "receive_timestamp",
        ),
        patch.object(
            FormatQuery,
            "_get_partition_dates",
            return_value=partition_dates,
        ),
    ):
        strategy2 = BatchStepCustom(
            max_batch_size=8,
            max_batch_time=1000,
            next_step=format_query2,
            increment_by=increment_by,
        )
        strategy2.submit(_make_single_message())
        strategy2.join(2.0)

    # Second submission: all partitions already tracked, so 0 calls
    assert mock_execute.call_count == 0
    assert commit_step.submit.call_count == 1


@patch("snuba.lw_deletions.strategy._num_ongoing_mutations", return_value=1)
@patch("snuba.lw_deletions.strategy._execute_query")
@pytest.mark.redis_db
def test_split_by_partition_fallback(mock_execute: Mock, mock_num_mutations: Mock) -> None:
    """
    When partition splitting is enabled but system.parts returns no partitions,
    fall back to un-split DELETE (1 call per table).
    """
    commit_step = Mock()
    metrics = Mock()
    storage = get_writable_storage(StorageKey("search_issues"))

    state.set_config("lw_deletes_split_by_partition_search_issues", 1)

    format_query = FormatQuery(commit_step, storage, SearchIssuesFormatter(), metrics)

    with (
        patch.object(
            format_query,
            "_FormatQuery__partition_column",
            "receive_timestamp",
        ),
        patch.object(
            FormatQuery,
            "_get_partition_dates",
            return_value=[],
        ),
    ):
        strategy = BatchStepCustom(
            max_batch_size=8,
            max_batch_time=1000,
            next_step=format_query,
            increment_by=increment_by,
        )
        strategy.submit(_make_single_message())
        strategy.join(2.0)

    # Fallback: 1 table * 1 un-split call = 1
    assert mock_execute.call_count == 1
    assert commit_step.submit.call_count == 1


@patch("snuba.lw_deletions.strategy._num_ongoing_mutations", return_value=1)
@patch("snuba.lw_deletions.strategy._execute_query")
@pytest.mark.redis_db
def test_partition_date_filtering(mock_execute: Mock, mock_num_mutations: Mock) -> None:
    """
    When _get_partition_dates encounters partition dates outside the valid window
    (last 12 months through 7 days from now), those dates should be
    filtered out and a metric emitted for the skipped count.
    """
    metrics = Mock()
    storage = get_writable_storage(StorageKey("search_issues"))

    now = datetime.now()
    valid_date_1 = (now - timedelta(days=30)).strftime("%Y-%m-%d")
    valid_date_2 = (now - timedelta(days=60)).strftime("%Y-%m-%d")
    bogus_old = (now - timedelta(days=500)).strftime("%Y-%m-%d")
    bogus_future = (now + timedelta(days=30)).strftime("%Y-%m-%d")

    # Build mock system.parts response: each row is a (partition_string,) tuple
    # matching the (retention_days, 'YYYY-MM-DD') format used by search_issues
    mock_results = Mock()
    mock_results.results = [
        (f"(90, '{(now - timedelta(days=30)).strftime('%Y-%m-%d')}')",),
        (f"(90, '{(now - timedelta(days=60)).strftime('%Y-%m-%d')}')",),
        (f"(90, '{(now - timedelta(days=500)).strftime('%Y-%m-%d')}')",),
        (f"(90, '{(now + timedelta(days=30)).strftime('%Y-%m-%d')}')",),
    ]
    mock_connection = Mock()
    mock_connection.execute.return_value = mock_results

    format_query = FormatQuery(Mock(), storage, SearchIssuesFormatter(), metrics)
    cluster = storage.get_cluster()
    dummy_node = ClickhouseNode("localhost", 9000)

    with (
        patch.object(cluster, "get_local_nodes", return_value=[dummy_node]),
        patch.object(cluster, "get_node_connection", return_value=mock_connection),
    ):
        result = format_query._get_partition_dates("search_issues_local_v2")

    assert result == sorted([valid_date_1, valid_date_2])
    assert bogus_old not in result
    assert bogus_future not in result

    # Verify partition_date_filtered metric emitted with value=2
    filtered_calls = [
        c for c in metrics.increment.call_args_list if c[0][0] == "partition_date_filtered"
    ]
    assert len(filtered_calls) == 1
    assert filtered_calls[0][1]["value"] == 2


@patch("snuba.lw_deletions.strategy._execute_query")
@pytest.mark.redis_db
def test_local_inflight_counter_reconciles_with_ch(mock_execute: Mock) -> None:
    """
    Verify that the local in-flight counter reconciles with CH on each check.
    When CH returns 0 (stale), the counter resets, allowing more deletes.
    When CH returns a high value, the counter reflects it and blocks.
    """
    commit_step = Mock()
    metrics = Mock()
    storage = get_writable_storage(StorageKey("search_issues"))

    state.set_config("max_ongoing_mutations_for_delete", 3)
    state.set_config("lw_deletes_split_by_partition_search_issues", 1)
    state.set_config("lw_deletes_per_submit_budget", 100)

    partition_dates = ["2024-01-01", "2024-01-08", "2024-01-15"]

    format_query = FormatQuery(commit_step, storage, SearchIssuesFormatter(), metrics)

    # CH always returns 0 (stale) — all 3 partitions should execute
    with (
        patch("snuba.lw_deletions.strategy._num_ongoing_mutations", return_value=0),
        patch.object(
            format_query,
            "_FormatQuery__partition_column",
            "receive_timestamp",
        ),
        patch.object(
            FormatQuery,
            "_get_partition_dates",
            return_value=partition_dates,
        ),
    ):
        strategy = BatchStepCustom(
            max_batch_size=8,
            max_batch_time=1000,
            next_step=format_query,
            increment_by=increment_by,
        )
        strategy.submit(_make_single_message())
        strategy.join(2.0)

    # CH says 0 each time → local counter reconciles to 0 → all execute
    assert mock_execute.call_count == 3
    assert commit_step.submit.call_count == 1


@patch("snuba.lw_deletions.strategy._execute_query")
@pytest.mark.redis_db
def test_local_counter_increments_after_each_delete(mock_execute: Mock) -> None:
    """
    Verify that _execute_single_delete increments local inflight counter
    and that the counter reconciles with CH value on _check_ongoing_mutations.
    """
    from snuba.query.query_settings import HTTPQuerySettings

    metrics = Mock()
    storage = get_writable_storage(StorageKey("search_issues"))

    state.set_config("max_ongoing_mutations_for_delete", 5)

    format_query = FormatQuery(Mock(), storage, SearchIssuesFormatter(), metrics)

    with patch("snuba.lw_deletions.strategy._num_ongoing_mutations", return_value=0):
        # Initial CH check: reconciles local counter to 0
        format_query._check_ongoing_mutations()
        assert format_query._FormatQuery__local_inflight_count == 0  # type: ignore[attr-defined]

        # Simulate deletes — counter should increment
        query = Mock()
        format_query._execute_single_delete("test_table", query, HTTPQuerySettings())
        assert format_query._FormatQuery__local_inflight_count == 1  # type: ignore[attr-defined]

        format_query._execute_single_delete("test_table", query, HTTPQuerySettings())
        assert format_query._FormatQuery__local_inflight_count == 2  # type: ignore[attr-defined]

        # CH check with skip_throttle=True reconciles counter to CH value (0)
        format_query._check_ongoing_mutations(skip_throttle=True)
        assert format_query._FormatQuery__local_inflight_count == 0  # type: ignore[attr-defined]


@patch("snuba.lw_deletions.strategy._num_ongoing_mutations", return_value=1)
@patch("snuba.lw_deletions.strategy._execute_query")
@pytest.mark.redis_db
def test_per_submit_budget_exhaustion(mock_execute: Mock, mock_num_mutations: Mock) -> None:
    """
    When partition splitting produces more partitions than the per-submit budget,
    the budget should stop issuing DELETEs and raise TooManyOngoingMutationsError.
    On retry, Redis tracking skips already-processed partitions.
    We test _execute_delete_by_partition directly for precise control.
    """
    from snuba.query.query_settings import HTTPQuerySettings

    metrics = Mock()
    storage = get_writable_storage(StorageKey("search_issues"))

    state.set_config("lw_deletes_split_by_partition_search_issues", 1)
    state.set_config("lw_deletes_per_submit_budget", 2)

    partition_dates = ["2024-01-01", "2024-01-08", "2024-01-15", "2024-01-22", "2024-01-29"]

    format_query = FormatQuery(Mock(), storage, SearchIssuesFormatter(), metrics)
    conditions = SearchIssuesFormatter().format(
        [_get_message(10, {"project_id": [1], "group_id": [1]})]
    )
    where_clause = Mock()
    query_settings = HTTPQuerySettings()

    with (
        patch.object(
            format_query,
            "_FormatQuery__partition_column",
            "receive_timestamp",
        ),
        patch.object(
            FormatQuery,
            "_get_partition_dates",
            return_value=partition_dates,
        ),
        patch(
            "snuba.web.bulk_delete_query.construct_query",
            return_value=Mock(),
        ),
    ):
        table = "search_issues_local_v2"

        # First call: processes 2 partitions, budget exhausted
        with pytest.raises(TooManyOngoingMutationsError, match="per-submit budget"):
            format_query._execute_delete_by_partition(
                table, where_clause, query_settings, conditions
            )

        assert mock_execute.call_count == 2

        # Redis should have the 2 processed partitions tracked
        redis_client = get_redis_client(RedisClientKey.CONFIG)
        keys = list(redis_client.scan_iter("lw_delete_partitions:search_issues:*"))
        assert len(keys) == 1
        members = redis_client.smembers(keys[0])
        assert len(members) == 2

        # Retry: reset the per-submit counter (simulating a new submit() call)
        mock_execute.reset_mock()
        format_query._FormatQuery__mutations_issued_this_submit = 0  # type: ignore[attr-defined]

        with pytest.raises(TooManyOngoingMutationsError, match="per-submit budget"):
            format_query._execute_delete_by_partition(
                table, where_clause, query_settings, conditions
            )

        # 2 skipped via Redis, 2 more processed
        assert mock_execute.call_count == 2
        members = redis_client.smembers(keys[0])
        assert len(members) == 4

        # Final retry: 4 skipped, 1 remaining fits in budget
        mock_execute.reset_mock()
        format_query._FormatQuery__mutations_issued_this_submit = 0  # type: ignore[attr-defined]

        format_query._execute_delete_by_partition(table, where_clause, query_settings, conditions)

        assert mock_execute.call_count == 1
        members = redis_client.smembers(keys[0])
        assert len(members) == 5


@patch("snuba.lw_deletions.strategy._num_ongoing_mutations", return_value=1)
@patch("snuba.lw_deletions.strategy._execute_query")
@patch("snuba.lw_deletions.strategy.time.sleep")
@pytest.mark.redis_db
def test_inter_delete_delay(mock_sleep: Mock, mock_execute: Mock, mock_num_mutations: Mock) -> None:
    """
    When lw_delete_inter_mutation_delay_ms is set, time.sleep should be called
    after each _execute_single_delete.
    """
    commit_step = Mock()
    metrics = Mock()
    storage = get_writable_storage(StorageKey("search_issues"))

    state.set_config("lw_delete_inter_mutation_delay_ms", 200)

    strategy = BatchStepCustom(
        max_batch_size=8,
        max_batch_time=1000,
        next_step=FormatQuery(commit_step, storage, SearchIssuesFormatter(), metrics),
        increment_by=increment_by,
    )
    strategy.submit(_make_single_message())
    strategy.join(2.0)

    assert mock_execute.call_count == 1
    # sleep(0.2) should have been called once (200ms)
    mock_sleep.assert_called_once_with(0.2)


@patch("snuba.lw_deletions.strategy._num_ongoing_mutations", return_value=1)
@patch("snuba.lw_deletions.strategy._execute_query")
@patch("snuba.lw_deletions.strategy.time.sleep")
@pytest.mark.redis_db
def test_inter_delete_delay_disabled_by_default(
    mock_sleep: Mock, mock_execute: Mock, mock_num_mutations: Mock
) -> None:
    """
    By default (delay_ms=0), time.sleep should not be called.
    """
    commit_step = Mock()
    metrics = Mock()
    storage = get_writable_storage(StorageKey("search_issues"))

    # Explicitly set to 0 (the default)
    state.set_config("lw_delete_inter_mutation_delay_ms", 0)

    strategy = BatchStepCustom(
        max_batch_size=8,
        max_batch_time=1000,
        next_step=FormatQuery(commit_step, storage, SearchIssuesFormatter(), metrics),
        increment_by=increment_by,
    )
    strategy.submit(_make_single_message())
    strategy.join(2.0)

    assert mock_execute.call_count == 1
    mock_sleep.assert_not_called()
