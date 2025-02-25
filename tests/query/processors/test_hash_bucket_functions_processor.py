from copy import deepcopy

import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities.entity_key import EntityKey
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import binary_condition, column, literal
from snuba.query.expressions import Column, FunctionCall
from snuba.query.logical import Query
from snuba.query.processors.logical.hash_bucket_functions import (
    HashBucketFunctionTransformer,
)
from snuba.query.query_settings import HTTPQuerySettings

test_data = [
    (
        Query(
            QueryEntity(EntityKey.EAP_SPANS, ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "keys",
                    FunctionCall("alias", "mapKeys", (Column(None, None, "attr_str"),)),
                ),
                SelectedExpression(
                    "arrayElement",
                    FunctionCall(
                        None,
                        "arrayElement",
                        (Column(None, None, "attr_str"), literal("blah")),
                    ),
                ),
                SelectedExpression(
                    "unrelated",
                    Column(None, None, "column2"),
                ),
                SelectedExpression(
                    "unrelated_map",
                    FunctionCall(
                        "aliasunrelatedmap",
                        "mapKeys",
                        (Column(None, None, "attr_stru"),),
                    ),
                ),
            ],
        ),
        Query(
            QueryEntity(EntityKey.EAP_SPANS, ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "keys",
                    FunctionCall(
                        "alias",
                        "arrayConcat",
                        tuple(
                            FunctionCall(
                                None, "mapKeys", (Column(None, None, f"attr_str_{i}"),)
                            )
                            for i in range(5)
                        ),
                    ),
                ),
                SelectedExpression(
                    "arrayElement",
                    FunctionCall(
                        None,
                        "arrayElement",
                        (Column(None, None, "attr_str_2"), literal("blah")),
                    ),
                ),
                SelectedExpression(
                    "unrelated",
                    Column(None, None, "column2"),
                ),
                SelectedExpression(
                    "unrelated_map",
                    FunctionCall(
                        "aliasunrelatedmap",
                        "mapKeys",
                        (Column(None, None, "attr_stru"),),
                    ),
                ),
            ],
        ),
    ),
    (
        Query(
            QueryEntity(EntityKey.EAP_SPANS, ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "values",
                    FunctionCall(
                        "alias", "mapValues", (Column(None, None, "attr_str"),)
                    ),
                ),
                SelectedExpression(
                    "unrelated",
                    Column(None, None, "column2"),
                ),
                SelectedExpression(
                    "unrelated_map",
                    FunctionCall(
                        "aliasunrelatedmap",
                        "mapValues",
                        (Column(None, None, "attr_stru"),),
                    ),
                ),
            ],
        ),
        Query(
            QueryEntity(EntityKey.EAP_SPANS, ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "values",
                    FunctionCall(
                        "alias",
                        "arrayConcat",
                        tuple(
                            FunctionCall(
                                None,
                                "mapValues",
                                (Column(None, None, f"attr_str_{i}"),),
                            )
                            for i in range(5)
                        ),
                    ),
                ),
                SelectedExpression(
                    "unrelated",
                    Column(None, None, "column2"),
                ),
                SelectedExpression(
                    "unrelated_map",
                    FunctionCall(
                        "aliasunrelatedmap",
                        "mapValues",
                        (Column(None, None, "attr_stru"),),
                    ),
                ),
            ],
        ),
    ),
    (
        Query(
            QueryEntity(EntityKey.EAP_SPANS, ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "unrelated",
                    Column(None, None, "column2"),
                ),
            ],
            condition=binary_condition(
                "or",
                f.equals(
                    Column(None, None, "unrelated1"), Column(None, None, "unrelated2")
                ),
                f.greaterThan(
                    f.length(
                        FunctionCall(
                            "alias", "mapValues", (Column(None, None, "attr_str"),)
                        )
                    ),
                    literal(2),
                ),
            ),
        ),
        Query(
            QueryEntity(EntityKey.EAP_SPANS, ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "unrelated",
                    Column(None, None, "column2"),
                ),
            ],
            condition=binary_condition(
                "or",
                f.equals(
                    Column(None, None, "unrelated1"), Column(None, None, "unrelated2")
                ),
                f.greaterThan(
                    f.length(
                        FunctionCall(
                            "alias",
                            "arrayConcat",
                            tuple(
                                FunctionCall(
                                    None,
                                    "mapValues",
                                    (Column(None, None, f"attr_str_{i}"),),
                                )
                                for i in range(5)
                            ),
                        )
                    ),
                    literal(2),
                ),
            ),
        ),
    ),
    (
        Query(
            QueryEntity(EntityKey.EAP_SPANS, ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "unrelated",
                    Column(None, None, "column2"),
                ),
            ],
            condition=binary_condition(
                "or",
                f.mapContains(column("attr_str"), literal("blah"), alias="x"),
                f.mapContains(column("attr_strz"), literal("blah"), alias="z"),
            ),
        ),
        Query(
            QueryEntity(EntityKey.EAP_SPANS, ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "unrelated",
                    Column(None, None, "column2"),
                ),
            ],
            condition=binary_condition(
                "or",
                f.mapContains(column("attr_str_2"), literal("blah"), alias="x"),
                f.mapContains(column("attr_strz"), literal("blah"), alias="z"),
            ),
        ),
    ),
]


@pytest.mark.parametrize("pre_format, expected_query", test_data)
def test_format_expressions(pre_format: Query, expected_query: Query) -> None:
    copy = deepcopy(pre_format)
    HashBucketFunctionTransformer("attr_str", num_attribute_buckets=5).process_query(
        copy, HTTPQuerySettings()
    )
    assert copy.get_selected_columns() == expected_query.get_selected_columns()
    assert copy.get_groupby() == expected_query.get_groupby()
    assert copy.get_condition() == expected_query.get_condition()
