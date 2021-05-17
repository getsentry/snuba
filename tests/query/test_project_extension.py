from typing import Any, MutableMapping, Sequence, cast

import pytest

from snuba import state
from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities import EntityKey
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.project_extension import ProjectExtension
from snuba.query.types import Condition
from snuba.request.request_settings import HTTPRequestSettings
from snuba.schemas import validate_jsonschema


def build_in(project_column: str, projects: Sequence[int]) -> Expression:
    return FunctionCall(
        None,
        "in",
        (
            Column(f"_snuba_{project_column}", None, project_column),
            FunctionCall(None, "tuple", tuple([Literal(None, p) for p in projects])),
        ),
    )


project_extension_test_data = [
    ({"project": 2}, [("project_id", "IN", [2])], build_in("project_id", [2]),),
    (
        {"project": [2, 3]},
        [("project_id", "IN", [2, 3])],
        build_in("project_id", [2, 3]),
    ),
]


@pytest.mark.parametrize(
    "raw_data, expected_conditions, expected_ast_conditions",
    project_extension_test_data,
)
def test_project_extension_query_processing(
    raw_data: MutableMapping[str, Any],
    expected_conditions: Sequence[Condition],
    expected_ast_conditions: Expression,
) -> None:
    extension = ProjectExtension(project_column="project_id")
    schema = cast(MutableMapping[str, Any], extension.get_schema())
    valid_data = validate_jsonschema(raw_data, schema)
    query = Query(QueryEntity(EntityKey.EVENTS, ColumnSet([])))
    request_settings = HTTPRequestSettings()

    extension.get_processor().process_query(query, valid_data, request_settings)
    assert query.get_condition() == expected_ast_conditions


def test_project_extension_query_adds_rate_limits() -> None:
    extension = ProjectExtension(project_column="project_id")
    schema = cast(MutableMapping[str, Any], extension.get_schema())
    raw_data = {"project": [1, 2]}
    valid_data = validate_jsonschema(raw_data, schema)
    query = Query(QueryEntity(EntityKey.EVENTS, ColumnSet([])))
    request_settings = HTTPRequestSettings()

    num_rate_limits_before_processing = len(request_settings.get_rate_limit_params())
    extension.get_processor().process_query(query, valid_data, request_settings)

    rate_limits = request_settings.get_rate_limit_params()
    # make sure a rate limit was added by the processing
    assert len(rate_limits) == num_rate_limits_before_processing + 1

    most_recent_rate_limit = rate_limits[-1]
    assert most_recent_rate_limit.bucket == "1"
    assert most_recent_rate_limit.per_second_limit == 1000
    assert most_recent_rate_limit.concurrent_limit == 1000


def test_project_extension_project_rate_limits_are_overridden() -> None:
    extension = ProjectExtension(project_column="project_id")
    schema = cast(MutableMapping[str, Any], extension.get_schema())
    raw_data = {"project": [3, 4]}
    valid_data = validate_jsonschema(raw_data, schema)
    query = Query(QueryEntity(EntityKey.EVENTS, ColumnSet([])))
    request_settings = HTTPRequestSettings()
    state.set_config("project_per_second_limit_3", 5)
    state.set_config("project_concurrent_limit_3", 10)

    extension.get_processor().process_query(query, valid_data, request_settings)

    rate_limits = request_settings.get_rate_limit_params()
    most_recent_rate_limit = rate_limits[-1]

    assert most_recent_rate_limit.bucket == "3"
    assert most_recent_rate_limit.per_second_limit == 5
    assert most_recent_rate_limit.concurrent_limit == 10
