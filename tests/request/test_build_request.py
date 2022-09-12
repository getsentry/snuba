from datetime import datetime
from typing import Any, MutableMapping

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.factory import get_dataset
from snuba.query import SelectedExpression
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
    in_condition,
)
from snuba.query.data_source.simple import Entity
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request.schema import RequestSchema
from snuba.request.validation import build_request, parse_snql_query
from snuba.utils.metrics.timer import Timer

TESTS = [
    pytest.param(
        {
            "query": (
                "MATCH (events) "
                "SELECT count() AS count BY time "
                "WHERE "
                "project_id IN tuple(1) AND "
                "timestamp >= toDateTime('2011-07-01T19:54:15') AND"
                "timestamp < toDateTime('2018-07-06T19:54:15') "
                "LIMIT 1000 "
                "GRANULARITY 60"
            ),
            "parent_api": "<unknown>",
        },
        binary_condition(
            BooleanFunctions.AND,
            in_condition(
                Column("_snuba_project_id", None, "project_id"), [Literal(None, 1)]
            ),
            binary_condition(
                BooleanFunctions.AND,
                binary_condition(
                    ConditionFunctions.GTE,
                    Column("_snuba_timestamp", None, "timestamp"),
                    Literal(None, datetime(2011, 7, 1, 19, 54, 15)),
                ),
                binary_condition(
                    ConditionFunctions.LT,
                    Column("_snuba_timestamp", None, "timestamp"),
                    Literal(None, datetime(2018, 7, 6, 19, 54, 15)),
                ),
            ),
        ),
        id="SnQL query",
    ),
]


@pytest.mark.parametrize("body, condition", TESTS)
def test_build_request(body: MutableMapping[str, Any], condition: Expression) -> None:
    dataset = get_dataset("events")
    entity = dataset.get_default_entity()
    schema = RequestSchema.build(HTTPQuerySettings)

    request = build_request(
        body,
        parse_snql_query,
        HTTPQuerySettings,
        schema,
        dataset,
        Timer("test"),
        "my_request",
    )

    expected_query = Query(
        from_clause=Entity(EntityKey.EVENTS, entity.get_data_model()),
        selected_columns=[
            SelectedExpression(
                name="time",
                expression=Column(
                    alias="_snuba_time", table_name=None, column_name="time"
                ),
            ),
            SelectedExpression("count", FunctionCall("_snuba_count", "count", tuple())),
        ],
        condition=condition,
        groupby=[Column("_snuba_time", None, "time")],
        limit=1000,
        granularity=60,
    )

    assert request.referrer == "my_request"
    assert dict(request.original_body) == body
    status, differences = request.query.equals(expected_query)
    assert status == True, f"Query mismatch: {differences}"
