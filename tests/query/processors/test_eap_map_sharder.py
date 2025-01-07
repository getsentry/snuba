from copy import deepcopy

import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities.entity_key import EntityKey
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import binary_condition, column, literal
from snuba.query.expressions import Column, FunctionCall, SubscriptableReference
from snuba.query.logical import Query
from snuba.query.processors.logical.eap_map_sharder import EAPMapSharder
from snuba.query.query_settings import HTTPQuerySettings
from snuba.utils.constants import ATTRIBUTE_BUCKETS

test_data = [
    (
        "attr_str",
        "attr_str_dest",
        "String",
        Query(
            QueryEntity(EntityKey.EAP_SPANS, ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "keys",
                    FunctionCall("alias", "mapKeys", (Column(None, None, "attr_str"),)),
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
                                None,
                                "mapKeys",
                                (Column(None, None, f"attr_str_dest_{i}"),),
                            )
                            for i in range(ATTRIBUTE_BUCKETS)
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
                        "mapKeys",
                        (Column(None, None, "attr_stru"),),
                    ),
                ),
            ],
        ),
    ),
    (
        "attr_str",
        "attr_str_dest",
        "String",
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
                                (Column(None, None, f"attr_str_dest_{i}"),),
                            )
                            for i in range(ATTRIBUTE_BUCKETS)
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
        "attr_str",
        "attr_str_dest",
        "String",
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
                                    (Column(None, None, f"attr_str_dest_{i}"),),
                                )
                                for i in range(ATTRIBUTE_BUCKETS)
                            ),
                        )
                    ),
                    literal(2),
                ),
            ),
        ),
    ),
    (
        "attr_i64",
        "attr_num",
        "Int64",
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
                f.mapContains(column("attr_i64"), literal("blah"), alias="y"),
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
                f.mapContains(column("attr_str"), literal("blah"), alias="x"),
                f.mapContains(column("attr_num_2"), literal("blah"), alias="y"),
                f.mapContains(column("attr_strz"), literal("blah"), alias="z"),
            ),
        ),
    ),
    (
        "attr_i64",
        "attr_num",
        "Int64",
        Query(
            QueryEntity(EntityKey.EAP_SPANS, ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "select_alias",
                    f.sumIf(
                        SubscriptableReference(
                            None,
                            column("attr_i64"),
                            literal("blah"),
                        ),
                        f.mapContains(
                            column("attr_i64"),
                            literal("blah"),
                        ),
                    ),
                ),
            ],
            condition=binary_condition(
                "or",
                f.mapContains(column("attr_str"), literal("blah"), alias="x"),
                f.mapContains(column("attr_i64"), literal("blah"), alias="y"),
                f.mapContains(column("attr_strz"), literal("blah"), alias="z"),
            ),
        ),
        Query(
            QueryEntity(EntityKey.EAP_SPANS, ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "select_alias",
                    f.sumIf(
                        f.CAST(
                            f.arrayElement(
                                column("attr_num_2"),
                                literal("blah"),
                            ),
                            "Int64",
                        ),
                        f.mapContains(
                            column("attr_num_2"),
                            literal("blah"),
                        ),
                    ),
                ),
            ],
            condition=binary_condition(
                "or",
                f.mapContains(column("attr_str"), literal("blah"), alias="x"),
                f.mapContains(column("attr_num_2"), literal("blah"), alias="y"),
                f.mapContains(column("attr_strz"), literal("blah"), alias="z"),
            ),
        ),
    ),
]


@pytest.mark.parametrize(
    "src_bucket_name, dest_bucket_name, data_type, pre_format, expected_query",
    test_data,
)
def test_eap_map_sharder(
    src_bucket_name: str,
    dest_bucket_name: str,
    data_type: str,
    pre_format: Query,
    expected_query: Query,
) -> None:
    copy = deepcopy(pre_format)
    EAPMapSharder(src_bucket_name, dest_bucket_name, data_type).process_query(
        copy, HTTPQuerySettings()
    )
    assert copy.get_selected_columns() == expected_query.get_selected_columns()
    assert copy.get_groupby() == expected_query.get_groupby()
    assert copy.get_condition() == expected_query.get_condition()
