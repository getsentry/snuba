from copy import deepcopy
from typing import Mapping

import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities.entity_key import EntityKey
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, literal
from snuba.query.expressions import FunctionCall, SubscriptableReference
from snuba.query.logical import Query
from snuba.query.processors.logical.eap_map_access_remapper import (
    EAPClickhouseColumnRemapper,
)
from snuba.query.query_settings import HTTPQuerySettings

test_data = [
    (
        "attr_i64",
        {"sentry.blah": "blahdest"},
        "Int64",
        Query(
            QueryEntity(EntityKey.EAP_SPANS, ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "selectedalias",
                    f.sum(
                        SubscriptableReference(
                            None, column("attr_i64"), literal("sentry.blah")
                        ),
                        alias="alias",
                    ),
                ),
                SelectedExpression(
                    "unrelated",
                    f.sum(
                        SubscriptableReference(
                            None, column("attr_i64"), literal("blahz")
                        ),
                        alias="alias",
                    ),
                ),
            ],
        ),
        Query(
            QueryEntity(EntityKey.EAP_SPANS, ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "selectedalias",
                    f.sum(
                        f.CAST(column("blahdest"), "Int64"),
                        alias="alias",
                    ),
                ),
                SelectedExpression(
                    "unrelated",
                    f.sum(
                        SubscriptableReference(
                            None, column("attr_i64"), literal("blahz")
                        ),
                        alias="alias",
                    ),
                ),
            ],
        ),
    ),
    (
        "attr_i64",
        {"sentry.blah": "blahdest"},
        "hex",
        Query(
            QueryEntity(EntityKey.EAP_SPANS, ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "selectedalias",
                    FunctionCall(
                        "alias",
                        "sum",
                        (
                            SubscriptableReference(
                                None, column("attr_i64"), literal("sentry.blah")
                            ),
                        ),
                    ),
                ),
                SelectedExpression(
                    "unrelated",
                    f.sum(
                        SubscriptableReference(
                            None, column("attr_i64"), literal("blahz")
                        ),
                        alias="alias",
                    ),
                ),
            ],
        ),
        Query(
            QueryEntity(EntityKey.EAP_SPANS, ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "selectedalias",
                    f.sum(f.hex(column("blahdest")), alias="alias"),
                ),
                SelectedExpression(
                    "unrelated",
                    f.sum(
                        SubscriptableReference(
                            None, column("attr_i64"), literal("blahz")
                        ),
                        alias="alias",
                    ),
                ),
            ],
        ),
    ),
]


@pytest.mark.parametrize(
    "bucket_name, keys, data_type, pre_format, expected_query",
    test_data,
)
def test_eap_column_remapper(
    bucket_name: str,
    keys: Mapping[str, str],
    data_type: str,
    pre_format: Query,
    expected_query: Query,
) -> None:
    copy = deepcopy(pre_format)
    EAPClickhouseColumnRemapper(bucket_name, keys, data_type).process_query(
        copy, HTTPQuerySettings()
    )
    assert copy.get_selected_columns() == expected_query.get_selected_columns()
    assert copy.get_groupby() == expected_query.get_groupby()
    assert copy.get_condition() == expected_query.get_condition()
