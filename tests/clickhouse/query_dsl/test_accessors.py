from datetime import datetime

import pytest

from snuba.clickhouse.query_dsl.accessors import get_object_ids_in_query_ast
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import SelectedExpression
from snuba.query.conditions import BooleanFunctions, binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query


@pytest.mark.parametrize(
    ["obj_condition", "expected_project_ids"],
    [
        pytest.param(
            FunctionCall(
                None,
                "equals",
                (
                    Column("_snuba_project_id", None, "project_id"),
                    Literal(None, 1),
                ),
            ),
            set([1]),
            id="equals_case",
        ),
        pytest.param(
            FunctionCall(
                None,
                "in",
                (
                    Column("_snuba_project_id", None, "project_id"),
                    FunctionCall(
                        None, "array", tuple([Literal(None, i) for i in range(1, 5)])
                    ),
                ),
            ),
            set([1, 2, 3, 4]),
            id="in_case",
        ),
        pytest.param(
            Literal(None, 1),
            set([]),
            id="empty_case",
        ),
    ],
)
def test_get_object_ids_in_query_ast(obj_condition, expected_project_ids):
    where_clause = binary_condition(
        BooleanFunctions.AND,
        FunctionCall(
            None,
            "greaterOrEquals",
            (
                Column("_snuba_timestamp", None, "timestamp"),
                Literal(None, datetime(2021, 1, 1, 0, 0)),
            ),
        ),
        binary_condition(
            BooleanFunctions.AND,
            FunctionCall(
                None,
                "less",
                (
                    Column("_snuba_timestamp", None, "timestamp"),
                    Literal(None, datetime(2021, 1, 2, 0, 0)),
                ),
            ),
            obj_condition,
        ),
    )
    query = Query(
        QueryEntity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()),
        selected_columns=[
            SelectedExpression("platform", Column("_snuba_platform", None, "platform")),
        ],
        groupby=[],
        condition=where_clause,
    )
    project_ids = get_object_ids_in_query_ast(query, "project_id")
    assert project_ids == expected_project_ids

    # test the case where the from clause is not set
    query = Query(
        None,
        selected_columns=[
            SelectedExpression("platform", Column("_snuba_platform", None, "platform")),
        ],
        groupby=[],
        condition=where_clause,
    )
    project_ids = get_object_ids_in_query_ast(query, "project_id")
    assert project_ids == expected_project_ids
