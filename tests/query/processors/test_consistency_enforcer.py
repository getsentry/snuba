import pytest
from sentry_options.testing import override_options

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter.query import format_query
from snuba.clickhouse.query import Query
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column
from snuba.query.processors.physical.consistency_enforcer import (
    ConsistencyEnforcerProcessor,
)
from snuba.query.query_settings import HTTPQuerySettings


def _query() -> Query:
    return Query(
        Table(
            "group_attributes_dist",
            ColumnSet([]),
            storage_key=StorageKey("group_attributes"),
        ),
        selected_columns=[
            SelectedExpression("group_id", Column("group_id", None, "group_id")),
        ],
    )


@pytest.mark.parametrize("killswitch", [False, True])
def test_consistency_enforcer_always_sets_final(killswitch: bool) -> None:
    query = _query()
    with override_options("snuba", {"disable_query_final": killswitch}):
        ConsistencyEnforcerProcessor().process_query(query, HTTPQuerySettings())
        assert query.get_from_clause().final
        assert "FROM group_attributes_dist FINAL" in format_query(query).get_sql()
