import pytest

from snuba.clickhouse.query import Query
from snuba.query.conditions import binary_condition
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.processors.physical.empty_tag_condition_processor import (
    EmptyTagConditionProcessor,
)
from snuba.query.query_settings import HTTPQuerySettings
from tests.query.processors.query_builders import build_query

test_data = [
    pytest.param(
        build_query(
            [],
            binary_condition(
                "notEquals",
                FunctionCall(
                    None,
                    "ifNull",
                    (
                        FunctionCall(
                            None,
                            "arrayElement",
                            (
                                Column(None, None, "tags.value"),
                                FunctionCall(
                                    None,
                                    "indexOf",
                                    (
                                        Column(None, None, "tags.key"),
                                        Literal(None, "query.error_reason"),
                                    ),
                                ),
                            ),
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
        build_query(
            [],
            binary_condition(
                "equals",
                FunctionCall(
                    None,
                    "ifNull",
                    (
                        FunctionCall(
                            None,
                            "arrayElement",
                            (
                                Column(None, None, "tags.value"),
                                FunctionCall(
                                    None,
                                    "indexOf",
                                    (
                                        Column(None, None, "tags.key"),
                                        Literal(None, "query.error_reason"),
                                    ),
                                ),
                            ),
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
        build_query(
            [],
            binary_condition(
                "equals",
                FunctionCall(
                    None,
                    "ifNull",
                    (
                        FunctionCall(
                            None,
                            "arrayElement",
                            (
                                Column(None, None, "tags.value"),
                                FunctionCall(
                                    None,
                                    "indexOf",
                                    (
                                        Column(None, None, "tags.key"),
                                        Literal(None, "query.error_reason"),
                                    ),
                                ),
                            ),
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
                    FunctionCall(
                        None,
                        "arrayElement",
                        (
                            Column(None, None, "tags.value"),
                            FunctionCall(
                                None,
                                "indexOf",
                                (
                                    Column(None, None, "tags.key"),
                                    Literal(None, "query.error_reason"),
                                ),
                            ),
                        ),
                    ),
                    Literal(None, ""),
                ),
            ),
            Literal(None, "some-reason"),
        ),
        id="only match conditions on empty strings",
    ),
]


@pytest.mark.parametrize("query, expected", test_data)  # type: ignore
def test_empty_tag_condition(query: Query, expected: Expression) -> None:
    query_settings = HTTPQuerySettings()
    processor = EmptyTagConditionProcessor(column_name="tags.key")
    processor.process_query(query, query_settings)
    assert query.get_condition() == expected
