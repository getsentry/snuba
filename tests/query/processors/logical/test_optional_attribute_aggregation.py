from copy import deepcopy

import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities.entity_key import EntityKey
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.dsl import CurriedFunctions as cf
from snuba.query.dsl import Functions as f
from snuba.query.dsl import NestedColumn, column
from snuba.query.logical import Query
from snuba.query.processors.logical.optional_attribute_aggregation import (
    OptionalAttributeAggregationTransformer,
)
from snuba.query.query_settings import HTTPQuerySettings

attr_num = NestedColumn("attr_num")

test_data = [
    (
        Query(
            QueryEntity(EntityKey.EAP_SPANS, ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "p90(x)", cf.quantile(0.9)(attr_num["x"], alias="p90(x)")
                ),
            ],
        ),
        Query(
            QueryEntity(EntityKey.EAP_SPANS, ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "p90(x)",
                    cf.quantileIf(0.9)(
                        attr_num["x"],
                        f.mapContains(column("attr_num", alias="_snuba_attr_num"), "x"),
                        alias="p90(x)",
                    ),
                ),
            ],
        ),
    ),
    (
        Query(
            QueryEntity(EntityKey.EAP_SPANS, ColumnSet([])),
            selected_columns=[
                SelectedExpression("avg(x)", f.avg(attr_num["x"], alias="avg(x)")),
            ],
        ),
        Query(
            QueryEntity(EntityKey.EAP_SPANS, ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "avg(x)",
                    f.avgIf(
                        attr_num["x"],
                        f.mapContains(column("attr_num", alias="_snuba_attr_num"), "x"),
                        alias="avg(x)",
                    ),
                ),
            ],
        ),
    ),
]


@pytest.mark.parametrize("pre_format, expected_query", test_data)
def test_query_processing(pre_format: Query, expected_query: Query) -> None:
    copy = deepcopy(pre_format)
    OptionalAttributeAggregationTransformer(
        attribute_column_names=["attr_num"],
        aggregation_names=["avg"],
        curried_aggregation_names=["quantile"],
    ).process_query(copy, HTTPQuerySettings())
    print(copy)
    print(expected_query)
    assert repr(copy) == repr(expected_query)

    assert copy.get_selected_columns() == expected_query.get_selected_columns()
    assert copy.get_groupby() == expected_query.get_groupby()
    assert copy.get_condition() == expected_query.get_condition()
