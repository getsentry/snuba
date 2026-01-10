from typing import Sequence, Type

import pytest
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.lw_deletions.bulk_delete_query import (
    DeleteQueryMessage,
    WireAttributeCondition,
)
from snuba.lw_deletions.formatters import (
    EAPItemsFormatter,
    Formatter,
    SearchIssuesFormatter,
)
from snuba.lw_deletions.types import ConditionsBag, ConditionsType


def create_delete_query_message(
    conditions: ConditionsType,
    attribute_conditions: dict[str, WireAttributeCondition] | None = None,
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
                ConditionsBag(
                    column_conditions={"project_id": [1], "group_id": [1, 2, 3, 4, 5, 6]}
                ),
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
                ConditionsBag(column_conditions={"project_id": [1], "group_id": [1, 2, 3]}),
                ConditionsBag(column_conditions={"project_id": [2], "group_id": [3]}),
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
                ConditionsBag(column_conditions={"project_id": [1], "group_id": [1, 2, 3, 4]}),
            ],
            SEARCH_ISSUES_FORMATTER,
            id="search_issues_dedupe_group_ids_in_same_project",
        ),
    ],
)
def test_search_issues_formatter(
    messages: Sequence[DeleteQueryMessage],
    expected_formatted: Sequence[ConditionsBag],
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
                ConditionsBag(column_conditions={"project_id": [1], "trace_id": [1, 2, 3]}),
                ConditionsBag(column_conditions={"project_id": [1], "trace_id": [4, 5, 6]}),
            ],
            EAPItemsFormatter,
            id="identity does basically nothing",
        ),
    ],
)
def test_eap_items_formatter_identity_conditions(
    messages: Sequence[DeleteQueryMessage],
    expected_formatted: Sequence[ConditionsBag],
    formatter: Type[Formatter],
) -> None:
    formatted = formatter().format(messages)
    assert formatted == expected_formatted


def test_eap_items_formatter_with_attribute_conditions() -> None:
    # Create a message with attribute_conditions in wire format
    messages = [
        create_delete_query_message(
            conditions={"project_id": [1], "item_type": [1]},
            attribute_conditions={
                "group_id": {
                    "attr_key_type": AttributeKey.Type.TYPE_INT,
                    "attr_key_name": "group_id",
                    "attr_values": [12345, 67890],
                }
            },
            attribute_conditions_item_type=1,
        )
    ]

    formatter = EAPItemsFormatter()
    formatted = formatter.format(messages)

    # EAPItemsFormatter now returns ConditionsBag with deserialized AttributeConditions
    assert len(formatted) == 1
    bag = formatted[0]
    assert bag.column_conditions == {"project_id": [1], "item_type": [1]}
    assert bag.attribute_conditions is not None
    assert bag.attribute_conditions.item_type == 1
    assert "group_id" in bag.attribute_conditions.attributes
    attr_key, values = bag.attribute_conditions.attributes["group_id"]
    assert attr_key.type == AttributeKey.Type.TYPE_INT
    assert attr_key.name == "group_id"
    assert values == [12345, 67890]


def test_eap_items_formatter_multiple_attributes() -> None:
    messages = [
        create_delete_query_message(
            conditions={"project_id": [1], "item_type": [1]},
            attribute_conditions={
                "group_id": {
                    "attr_key_type": AttributeKey.Type.TYPE_INT,
                    "attr_key_name": "group_id",
                    "attr_values": [12345],
                },
                "transaction": {
                    "attr_key_type": AttributeKey.Type.TYPE_STRING,
                    "attr_key_name": "transaction",
                    "attr_values": ["test_transaction"],
                },
            },
            attribute_conditions_item_type=1,
        )
    ]

    formatter = EAPItemsFormatter()
    formatted = formatter.format(messages)

    assert len(formatted) == 1
    bag = formatted[0]
    assert bag.column_conditions == {"project_id": [1], "item_type": [1]}
    assert bag.attribute_conditions is not None
    assert bag.attribute_conditions.item_type == 1

    # Check the AttributeKey types and values
    group_key, group_vals = bag.attribute_conditions.attributes["group_id"]
    assert group_key.type == AttributeKey.Type.TYPE_INT
    assert group_vals == [12345]

    txn_key, txn_vals = bag.attribute_conditions.attributes["transaction"]
    assert txn_key.type == AttributeKey.Type.TYPE_STRING
    assert txn_vals == ["test_transaction"]


def test_eap_items_formatter_with_float_attributes() -> None:
    messages = [
        create_delete_query_message(
            conditions={"project_id": [1], "item_type": [1]},
            attribute_conditions={
                "duration": {
                    "attr_key_type": AttributeKey.Type.TYPE_FLOAT,
                    "attr_key_name": "duration",
                    "attr_values": [123.45, 678.90],
                }
            },
            attribute_conditions_item_type=1,
        )
    ]

    formatter = EAPItemsFormatter()
    formatted = formatter.format(messages)

    assert len(formatted) == 1
    bag = formatted[0]
    assert bag.column_conditions == {"project_id": [1], "item_type": [1]}
    assert bag.attribute_conditions is not None

    dur_key, dur_vals = bag.attribute_conditions.attributes["duration"]
    assert dur_key.type == AttributeKey.Type.TYPE_FLOAT
    assert dur_vals == [123.45, 678.90]


def test_eap_items_formatter_with_bool_attributes() -> None:
    messages = [
        create_delete_query_message(
            conditions={"project_id": [1], "item_type": [1]},
            attribute_conditions={
                "is_error": {
                    "attr_key_type": AttributeKey.Type.TYPE_BOOLEAN,
                    "attr_key_name": "is_error",
                    "attr_values": [True, False],
                }
            },
            attribute_conditions_item_type=1,
        )
    ]

    formatter = EAPItemsFormatter()
    formatted = formatter.format(messages)

    assert len(formatted) == 1
    bag = formatted[0]
    assert bag.column_conditions == {"project_id": [1], "item_type": [1]}
    assert bag.attribute_conditions is not None

    err_key, err_vals = bag.attribute_conditions.attributes["is_error"]
    assert err_key.type == AttributeKey.Type.TYPE_BOOLEAN
    assert err_vals == [True, False]
