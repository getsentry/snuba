from __future__ import annotations

import pytest

from snuba.clickhouse.columns import UUID, ColumnSet, DateTime, String, UInt
from snuba.clickhouse.columns import SchemaModifiers as Mods
from snuba.clickhouse.formatter.query import format_query
from snuba.clickhouse.query import Query
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Table
from snuba.query.dsl import and_cond, in_cond
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.processors.physical.subquery_filter_optimizer import (
    SubqueryFilterOptimizer,
)
from snuba.query.query_settings import HTTPQuerySettings

COLUMNS = ColumnSet(
    [
        ("project_id", UInt(64)),
        ("timestamp", DateTime()),
        ("replay_id", UUID()),
        ("segment_id", UInt(16, Mods(nullable=True))),
        ("environment", String(Mods(nullable=True))),
        ("user_id", String(Mods(nullable=True))),
        ("user_email", String(Mods(nullable=True))),
        ("retention_days", UInt(16)),
    ]
)


def _col(name: str) -> Column:
    return Column(f"_snuba_{name}", None, name)


def _base_where() -> Expression:
    return and_cond(
        FunctionCall(None, "equals", (_col("project_id"), Literal(None, 42))),
        in_cond(
            _col("environment"),
            FunctionCall(None, "tuple", (Literal(None, "debug"), Literal(None, "release"))),
        ),
    )


def _build_query(having: Expression | None) -> Query:
    return Query(
        from_clause=Table("replays_local", COLUMNS, storage_key=StorageKey("replays")),
        selected_columns=[
            SelectedExpression("replay_id", _col("replay_id")),
            SelectedExpression(
                "max_segment_id", FunctionCall("_snuba_max", "max", (_col("segment_id"),))
            ),
        ],
        condition=_base_where(),
        groupby=[_col("replay_id")],
        having=having,
        order_by=[OrderBy(OrderByDirection.ASC, FunctionCall(None, "min", (_col("timestamp"),)))],
        limit=100,
    )


def _exists_having(column_name: str, value: str, op: str = "notEquals", rhs: int = 0) -> Expression:
    return FunctionCall(
        None,
        op,
        (
            FunctionCall(
                None,
                "sum",
                (FunctionCall(None, "equals", (_col(column_name), Literal(None, value))),),
            ),
            Literal(None, rhs),
        ),
    )


@pytest.mark.parametrize(
    "op,rhs",
    [
        ("notEquals", 0),
        ("greater", 0),
        ("greaterOrEquals", 1),
    ],
)
def test_rewrites_positive_existence(op: str, rhs: int) -> None:
    query = _build_query(_exists_having("user_id", "xyz", op=op, rhs=rhs))
    SubqueryFilterOptimizer("replay_id", ["user_id", "user_email"]).process_query(
        query, HTTPQuerySettings()
    )
    sql = format_query(query).get_sql()

    # A semi-join subquery on replay_id is added with the user_id predicate so
    # the bloom-filter skip indexes can prune granules.
    assert "globalIn(replay_id, (SELECT replay_id FROM replays_local" in sql
    assert "equals((user_id AS _snuba_user_id), 'xyz')" in sql
    # The original HAVING is preserved (the subquery is purely an accelerator).
    assert query.get_having() is not None
    assert "HAVING" in sql


def test_use_plain_in_when_not_global() -> None:
    query = _build_query(_exists_having("user_id", "xyz"))
    SubqueryFilterOptimizer("replay_id", ["user_id"], use_global_in=False).process_query(
        query, HTTPQuerySettings()
    )
    sql = format_query(query).get_sql()
    assert "in(replay_id, (SELECT replay_id FROM replays_local" in sql
    assert "globalIn(replay_id" not in sql


def test_multiple_existence_filters_each_get_a_subquery() -> None:
    query = _build_query(
        and_cond(
            _exists_having("user_id", "xyz"),
            _exists_having("user_email", "a@b.com"),
        )
    )
    SubqueryFilterOptimizer("replay_id", ["user_id", "user_email"]).process_query(
        query, HTTPQuerySettings()
    )
    sql = format_query(query).get_sql()
    assert sql.count("globalIn(replay_id, (SELECT replay_id FROM replays_local") == 2
    assert "equals((user_id AS _snuba_user_id), 'xyz')" in sql
    assert "equals((user_email AS _snuba_user_email), 'a@b.com')" in sql


def test_no_having_is_noop() -> None:
    query = _build_query(None)
    before = format_query(query).get_sql()
    SubqueryFilterOptimizer("replay_id", ["user_id"]).process_query(query, HTTPQuerySettings())
    assert format_query(query).get_sql() == before


def test_column_not_in_filter_list_is_noop() -> None:
    # segment_id is not a configured (bloom-filter) filter column.
    query = _build_query(_exists_having("segment_id", "1"))
    before = format_query(query).get_sql()
    SubqueryFilterOptimizer("replay_id", ["user_id", "user_email"]).process_query(
        query, HTTPQuerySettings()
    )
    assert format_query(query).get_sql() == before


def test_non_existence_having_is_noop() -> None:
    # `sum(...) == 0` means "no matching row" -- the IN rewrite would be wrong,
    # so it must be skipped.
    query = _build_query(_exists_having("user_id", "xyz", op="equals", rhs=0))
    before = format_query(query).get_sql()
    SubqueryFilterOptimizer("replay_id", ["user_id"]).process_query(query, HTTPQuerySettings())
    assert format_query(query).get_sql() == before
