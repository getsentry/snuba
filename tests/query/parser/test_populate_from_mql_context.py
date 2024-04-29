from datetime import datetime

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.dsl import and_cond, column, equals, function_call, literal
from snuba.query.logical import Query
from snuba.query.mql.mql_context import MQLContext
from snuba.query.mql.parser import populate_query_from_mql_context


@pytest.fixture
def from_distributions() -> QueryEntity:
    # TODO: its too complicated to get an entity for a query imo
    return QueryEntity(
        EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
        get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
    )


"""
Many of the individual units of the pipeline were not tested and relied on the E2E tests,
since we have removed a large number of the E2E tests, we need unit tests to make up for the lost coverage.

This is what they would look like for the populate_query_from_mql_context stage.

For our usage, I think for now, just general case functionality tests should be sufficient for each component.
However it is clear to see that this could be extended with tests covering more edge case behavior,
which shows how this approach could actually increase the coverage of our tests. (as i mentioned in the slides)
"""


def test_basic(from_distributions: QueryEntity) -> None:
    query = Query(
        from_distributions,
        selected_columns=[
            SelectedExpression("aggregate_value", function_call("sum", column("value")))
        ],
        condition=equals(
            column("metric_id"), literal("d:transactions/duration@millisecond")
        ),
    )
    context = {
        "start": "2021-01-01T00:00:00",
        "end": "2021-01-02T00:00:00",
        "rollup": {
            "orderby": "ASC",
            "granularity": 60,
            "interval": None,
            "with_totals": None,
        },
        "scope": {"org_ids": [1], "project_ids": [1], "use_case_id": "transactions"},
        "limit": None,
        "offset": None,
        "indexer_mappings": {"d:transactions/duration@millisecond": 123456},
    }
    expected_query = Query(
        from_distributions,
        selected_columns=[
            SelectedExpression("aggregate_value", function_call("sum", column("value")))
        ],
        order_by=[OrderBy(OrderByDirection.ASC, column("aggregate_value"))],
        condition=and_cond(
            equals(column("granularity"), literal(60)),
            and_cond(
                and_cond(
                    function_call(
                        "in", column("project_id"), function_call("tuple", literal(1))
                    ),
                    and_cond(
                        function_call(
                            "in", column("org_id"), function_call("tuple", literal(1))
                        ),
                        equals(column("use_case_id"), literal("transactions")),
                    ),
                ),
                and_cond(
                    and_cond(
                        function_call(
                            "greaterOrEquals",
                            column("timestamp"),
                            literal(datetime(year=2021, month=1, day=1)),
                        ),
                        function_call(
                            "less",
                            column("timestamp"),
                            literal(datetime(year=2021, month=1, day=2)),
                        ),
                    ),
                    equals(
                        column("metric_id"),
                        literal("d:transactions/duration@millisecond"),
                    ),
                ),
            ),
        ),
        limit=1000,
    )
    expected_context = MQLContext.from_dict(context)
    actual_query, actual_context = populate_query_from_mql_context(query, context)
    assert actual_query == expected_query and actual_context == expected_context
