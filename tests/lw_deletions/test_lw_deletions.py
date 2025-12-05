from __future__ import annotations

from datetime import datetime
from typing import Iterator
from unittest.mock import Mock, patch

import pytest
import rapidjson
from arroyo.backends.kafka import KafkaPayload
from arroyo.types import BrokerValue, Message, Partition, Topic

from snuba import state
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.lw_deletions.batching import BatchStepCustom
from snuba.lw_deletions.formatters import SearchIssuesFormatter
from snuba.lw_deletions.strategy import FormatQuery, increment_by
from snuba.lw_deletions.types import ConditionsType
from snuba.utils.streams.topics import Topic as SnubaTopic
from snuba.web.bulk_delete_query import DeleteQueryMessage

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
