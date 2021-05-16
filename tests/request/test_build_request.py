from datetime import datetime
from typing import Any, MutableMapping

import pytest

from snuba.datasets.entities import EntityKey
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
from snuba.request import Language
from snuba.request.request_settings import HTTPRequestSettings
from snuba.request.schema import RequestSchema
from snuba.request.validation import (
    build_legacy_parser,
    build_request,
    build_snql_parser,
)
from snuba.utils.metrics.timer import Timer

TESTS = [
    pytest.param(
        {
            "project": [1],
            "selected_columns": [],
            "aggregations": [["count()", "", "count"]],
            "conditions": [],
            "from_date": "2011-07-01T19:54:15",
            "to_date": "2018-07-06T19:54:15",
            "granularity": 60,
            "groupby": ["time"],
            "having": [],
            "limit": 1000,
            "totals": False,
        },
        Language.LEGACY,
        binary_condition(
            BooleanFunctions.AND,
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
            in_condition(
                Column("_snuba_project_id", None, "project_id"), [Literal(None, 1)]
            ),
        ),
        id="Legacy query",
    ),
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
        },
        Language.SNQL,
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


@pytest.mark.parametrize("body, language, condition", TESTS)
def test_build_request(
    body: MutableMapping[str, Any], language: Language, condition: Expression
) -> None:
    dataset = get_dataset("events")
    entity = dataset.get_default_entity()
    schema = RequestSchema.build_with_extensions(
        entity.get_extensions(), HTTPRequestSettings, language,
    )

    request = build_request(
        body,
        build_legacy_parser(dataset)
        if language == Language.LEGACY
        else build_snql_parser([]),
        HTTPRequestSettings,
        schema,
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
    assert dict(request.body) == body
    status, differences = request.query.equals(expected_query)
    assert status == True, f"Query mismatch: {differences}"
