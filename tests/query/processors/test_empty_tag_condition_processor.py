import pytest

from snuba.query.conditions import binary_condition
from snuba.query.expressions import (
    Column,
    Expression,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query
from snuba.query.processors.empty_tag_condition_processor import (
    EmptyTagConditionProcessor,
)
from snuba.request.request_settings import HTTPRequestSettings

test_data = [
    pytest.param(
        Query(
            None,
            selected_columns=[],
            condition=binary_condition(
                "notEquals",
                FunctionCall(
                    None,
                    "ifNull",
                    (
                        SubscriptableReference(
                            "_snuba_tags[query.error_reason]",
                            Column("_snuba_tags", None, "tags"),
                            Literal(None, "query.error_reason"),
                        ),
                        Literal(None, ""),
                    ),
                ),
                Literal(None, ""),
            ),
        ),
        FunctionCall(
            None,
            "has",
            (Column(None, None, "tags.key"), Literal(None, "query.error_reason")),
        ),
        id="not equals on empty string converted to has on tags key",
    ),
    pytest.param(
        Query(
            None,
            selected_columns=[],
            condition=binary_condition(
                "equals",
                FunctionCall(
                    None,
                    "ifNull",
                    (
                        SubscriptableReference(
                            "_snuba_tags[query.error_reason]",
                            Column("_snuba_tags", None, "tags"),
                            Literal(None, "query.error_reason"),
                        ),
                        Literal(None, ""),
                    ),
                ),
                Literal(None, ""),
            ),
        ),
        FunctionCall(
            None,
            "not",
            (
                FunctionCall(
                    None,
                    "has",
                    (
                        Column(None, None, "tags.key"),
                        Literal(None, "query.error_reason"),
                    ),
                ),
            ),
        ),
        id="equals on empty string converted to not has on tags key",
    ),
    pytest.param(
        Query(
            None,
            selected_columns=[],
            condition=binary_condition(
                "equals",
                FunctionCall(
                    None,
                    "ifNull",
                    (
                        SubscriptableReference(
                            "_snuba_tags[query.error_reason]",
                            Column("_snuba_tags", None, "tags"),
                            Literal(None, "query.error_reason"),
                        ),
                        Literal(None, ""),
                    ),
                ),
                Literal(None, "some-reason"),
            ),
        ),
        binary_condition(
            "equals",
            FunctionCall(
                None,
                "ifNull",
                (
                    SubscriptableReference(
                        "_snuba_tags[query.error_reason]",
                        Column("_snuba_tags", None, "tags"),
                        Literal(None, "query.error_reason"),
                    ),
                    Literal(None, ""),
                ),
            ),
            Literal(None, "some-reason"),
        ),
        id="only match conditions on empty strings",
    ),
]


@pytest.mark.parametrize("query, expected", test_data)
def test_empty_tag_condition(query: Query, expected: Expression) -> None:
    request_settings = HTTPRequestSettings()
    processor = EmptyTagConditionProcessor()
    processor.process_query(query, request_settings)
    assert query.get_condition() == expected
