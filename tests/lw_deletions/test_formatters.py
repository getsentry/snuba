from typing import Any, Sequence, Type

import pytest

from snuba.lw_deletions.formatters import (
    EAPItemsFormatter,
    Formatter,
    SearchIssuesFormatter,
)
from snuba.utils.hashes import fnv_1a
from snuba.web.bulk_delete_query import DeleteQueryMessage
from snuba.web.delete_query import ConditionsType


def create_delete_query_message(
    conditions: ConditionsType,
    attribute_conditions: dict[str, list[Any]] | None = None,
    attribute_conditions_item_type: int | None = None,
) -> DeleteQueryMessage:
    msg = DeleteQueryMessage(
        rows_to_delete=1,
        tenant_ids={},
        conditions=conditions,
        storage_name="search_issues",
    )
    if attribute_conditions is not None and attribute_conditions_item_type is not None:
        msg["attribute_conditions"] = attribute_conditions
        msg["attribute_conditions_item_type"] = attribute_conditions_item_type
    return msg


SEARCH_ISSUES_FORMATTER = SearchIssuesFormatter


@pytest.mark.parametrize(
    "messages, expected_formatted, formatter",
    [
        pytest.param(
            [
                create_delete_query_message({"project_id": [1], "group_id": [1, 2, 3]}),
                create_delete_query_message({"project_id": [1], "group_id": [4, 5, 6]}),
            ],
            [
                {"project_id": [1], "group_id": [1, 2, 3, 4, 5, 6]},
            ],
            SEARCH_ISSUES_FORMATTER,
            id="search_issues_combine_group_ids_same_project",
        ),
        pytest.param(
            [
                create_delete_query_message({"project_id": [1], "group_id": [1, 2, 3]}),
                create_delete_query_message({"project_id": [2], "group_id": [3]}),
            ],
            [
                {"project_id": [1], "group_id": [1, 2, 3]},
                {"project_id": [2], "group_id": [3]},
            ],
            SEARCH_ISSUES_FORMATTER,
            id="search_issues_diff_projects_dont_combine",
        ),
        pytest.param(
            [
                create_delete_query_message({"project_id": [1], "group_id": [1, 2, 3]}),
                create_delete_query_message({"project_id": [1], "group_id": [2, 3, 4]}),
            ],
            [
                {"project_id": [1], "group_id": [1, 2, 3, 4]},
            ],
            SEARCH_ISSUES_FORMATTER,
            id="search_issues_dedupe_group_ids_in_same_project",
        ),
    ],
)
def test_search_issues_formatter(
    messages: Sequence[DeleteQueryMessage],
    expected_formatted: Sequence[ConditionsType],
    formatter: Type[Formatter],
) -> None:
    formatted = formatter().format(messages)
    assert formatted == expected_formatted


@pytest.mark.parametrize(
    "messages, expected_formatted, formatter",
    [
        pytest.param(
            [
                create_delete_query_message({"project_id": [1], "trace_id": [1, 2, 3]}),
                create_delete_query_message({"project_id": [1], "trace_id": [4, 5, 6]}),
            ],
            [
                {"project_id": [1], "trace_id": [1, 2, 3]},
                {"project_id": [1], "trace_id": [4, 5, 6]},
            ],
            EAPItemsFormatter,
            id="identity does basically nothing",
        ),
    ],
)
def test_identity_formatter(
    messages: Sequence[DeleteQueryMessage],
    expected_formatted: Sequence[ConditionsType],
    formatter: Type[Formatter],
) -> None:
    formatted = formatter().format(messages)
    assert formatted == expected_formatted


def test_eap_items_formatter_with_attribute_conditions() -> None:
    """Test that EAPItemsFormatter correctly resolves attribute_conditions to bucketed columns"""
    # Create a message with attribute_conditions
    messages = [
        create_delete_query_message(
            conditions={"project_id": [1], "item_type": [1]},
            attribute_conditions={"group_id": [12345, 67890]},
            attribute_conditions_item_type=1,
        )
    ]

    formatter = EAPItemsFormatter()
    formatted = formatter.format(messages)

    # Calculate the expected bucket for "group_id"
    expected_bucket = fnv_1a("group_id".encode("utf-8")) % 40
    expected_column = f"attributes_string_{expected_bucket}['group_id']"

    assert len(formatted) == 1
    assert formatted[0]["project_id"] == [1]
    assert formatted[0]["item_type"] == [1]
    assert formatted[0][expected_column] == [12345, 67890]


def test_eap_items_formatter_multiple_attributes() -> None:
    """Test that EAPItemsFormatter handles multiple attributes correctly"""
    messages = [
        create_delete_query_message(
            conditions={"project_id": [1], "item_type": [1]},
            attribute_conditions={
                "group_id": [12345],
                "transaction": ["test_transaction"],
            },
            attribute_conditions_item_type=1,
        )
    ]

    formatter = EAPItemsFormatter()
    formatted = formatter.format(messages)

    # Calculate expected buckets
    group_id_bucket = fnv_1a("group_id".encode("utf-8")) % 40
    transaction_bucket = fnv_1a("transaction".encode("utf-8")) % 40

    expected_group_id_column = f"attributes_string_{group_id_bucket}['group_id']"
    expected_transaction_column = f"attributes_string_{transaction_bucket}['transaction']"

    assert len(formatted) == 1
    assert formatted[0][expected_group_id_column] == [12345]
    assert formatted[0][expected_transaction_column] == ["test_transaction"]


def test_eap_items_formatter_without_attribute_conditions() -> None:
    """Test that EAPItemsFormatter works without attribute_conditions (backwards compatibility)"""
    messages = [
        create_delete_query_message(
            conditions={"project_id": [1], "trace_id": ["abc123"]},
        )
    ]

    formatter = EAPItemsFormatter()
    formatted = formatter.format(messages)

    assert len(formatted) == 1
    assert formatted[0] == {"project_id": [1], "trace_id": ["abc123"]}
