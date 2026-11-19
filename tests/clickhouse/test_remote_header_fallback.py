from __future__ import annotations

from datetime import datetime

import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.formatter.query import format_query
from snuba.clickhouse.query import Query
from snuba.clickhouse.remote_header_fallback import (
    is_remote_header_mismatch,
    wrap_in_projection_subquery,
)
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import LimitBy, OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, DangerousRawSQL, Expression, FunctionCall, Literal

ERRORS_DIST = Table("errors_dist", ColumnSet([]), storage_key=StorageKey("errors"))


def _col(name: str, alias: str | None = None) -> Column:
    return Column(alias, None, name)


def _f(name: str, *args: Expression, alias: str | None = None) -> FunctionCall:
    return FunctionCall(alias, name, args)


PROJECT_CONDITION = _f(
    "and",
    _f("equals", _col("project_id", "_snuba_project_id"), Literal(None, 4500)),
    _f(
        "greaterOrEquals",
        _col("timestamp", "_snuba_timestamp"),
        Literal(None, datetime(2026, 11, 17)),
    ),
)


def _sql(query: Query) -> str:
    rewritten = wrap_in_projection_subquery(query)
    assert rewritten is not None
    return format_query(rewritten).get_sql()


def test_unhandled_flag_query() -> None:
    # The group.unhandled-flag query from INC-2695.
    query = Query(
        ERRORS_DIST,
        selected_columns=[
            SelectedExpression("_snuba_group_id", _col("group_id", "_snuba_group_id")),
            SelectedExpression(
                "_snuba_unhandled",
                _f(
                    "argMax",
                    _f("has", _col("exception_stacks.mechanism_handled"), Literal(None, 0)),
                    _col("timestamp", "_snuba_timestamp"),
                    alias="_snuba_unhandled",
                ),
            ),
        ],
        prewhere=_f(
            "in",
            _col("group_id", "_snuba_group_id"),
            _f("tuple", Literal(None, 101), Literal(None, 102)),
        ),
        condition=PROJECT_CONDITION,
        groupby=[_col("group_id", "_snuba_group_id")],
        limit=1000,
    )
    assert _sql(query) == (
        "SELECT (_snuba_inner_0 AS _snuba_group_id), "
        "(argMax(has(_snuba_inner_1, 0), (_snuba_inner_2 AS _snuba_timestamp)) AS _snuba_unhandled) "
        "FROM (SELECT (group_id AS _snuba_inner_0), "
        "(`exception_stacks.mechanism_handled` AS _snuba_inner_1), "
        "(timestamp AS _snuba_inner_2) "
        "FROM errors_dist "
        "PREWHERE in((group_id AS _snuba_group_id), (101, 102)) "
        "WHERE equals((project_id AS _snuba_project_id), 4500) "
        "AND greaterOrEquals((timestamp AS _snuba_timestamp), "
        "toDateTime('2026-11-17T00:00:00', 'Universal'))) "
        "GROUP BY _snuba_group_id LIMIT 1000 OFFSET 0"
    )


def test_tag_keys_keeps_array_join_function_outside() -> None:
    tags_key = _f("arrayJoin", _col("tags.key"), alias="_snuba_tags_key")
    query = Query(
        ERRORS_DIST,
        selected_columns=[
            SelectedExpression("_snuba_tags_key", tags_key),
            SelectedExpression("_snuba_count", _f("count", alias="_snuba_count")),
        ],
        condition=PROJECT_CONDITION,
        groupby=[tags_key],
        order_by=[OrderBy(OrderByDirection.DESC, _f("count", alias="_snuba_count"))],
        limitby=LimitBy(1, [tags_key]),
        limit=10,
        offset=5,
        totals=True,
    )
    assert _sql(query) == (
        "SELECT (arrayJoin(_snuba_inner_0) AS _snuba_tags_key), (count() AS _snuba_count) "
        "FROM (SELECT (`tags.key` AS _snuba_inner_0) FROM errors_dist "
        "WHERE equals((project_id AS _snuba_project_id), 4500) "
        "AND greaterOrEquals((timestamp AS _snuba_timestamp), "
        "toDateTime('2026-11-17T00:00:00', 'Universal'))) "
        "GROUP BY _snuba_tags_key WITH TOTALS "
        "ORDER BY _snuba_count DESC "
        "LIMIT 1 BY _snuba_tags_key "
        "LIMIT 10 OFFSET 5"
    )


def test_final_and_sampling_stay_on_the_inner_table() -> None:
    table = Table(
        "errors_dist",
        ColumnSet([]),
        storage_key=StorageKey("errors"),
        final=True,
        sampling_rate=0.1,
    )
    query = Query(
        table,
        selected_columns=[SelectedExpression("tags_key", _col("tags.key"))],
        order_by=[OrderBy(OrderByDirection.ASC, _col("timestamp"))],
    )
    assert _sql(query) == (
        "SELECT _snuba_inner_0 FROM (SELECT (`tags.key` AS _snuba_inner_0), "
        "(timestamp AS _snuba_inner_1) FROM errors_dist FINAL SAMPLE 0.1) "
        "ORDER BY _snuba_inner_1 ASC"
    )


@pytest.mark.parametrize(
    "query",
    [
        pytest.param(
            Query(
                ERRORS_DIST,
                selected_columns=[SelectedExpression("k", _col("tags.key"))],
                array_join=[_col("tags.key")],
            ),
            id="array-join-clause",
        ),
        pytest.param(
            Query(
                ERRORS_DIST,
                selected_columns=[SelectedExpression("c", _f("count"))],
                condition=_f("equals", _f("arrayJoin", _col("tags.key")), Literal(None, "a")),
            ),
            id="array-join-in-where",
        ),
        pytest.param(
            Query(
                ERRORS_DIST,
                selected_columns=[SelectedExpression("x", DangerousRawSQL(None, "tags.key"))],
            ),
            id="raw-sql",
        ),
        pytest.param(
            Query(ERRORS_DIST, selected_columns=[SelectedExpression("c", _f("count"))]),
            id="no-columns",
        ),
    ],
)
def test_unsupported_shapes_are_not_rewritten(query: Query) -> None:
    assert wrap_in_projection_subquery(query) is None


@pytest.mark.parametrize(
    "message, expected",
    [
        pytest.param(
            # Query node on 26.8, storage on 25.8 (from the mixed-version CI job).
            "Received ClickHouse exception, code: 10, server response: Code: 10. "
            "DB::Exception: Not found column arrayJoin(__table1.`tags.key`) in block. "
            "There are only columns: arrayJoin(__table1.tags.key), count(): "
            "While executing Remote. (NOT_FOUND_COLUMN_IN_BLOCK) (version 26.8.36.3)",
            True,
            id="new-query-node",
        ),
        pytest.param(
            "Code: 10. DB::Exception: Not found column argMax(has(__table1.exception_stacks."
            "mechanism_handled, 0_UInt8), __table1.timestamp) in block. There are only "
            "columns: __table1.group_id, argMax(has(__table1.`exception_stacks."
            "mechanism_handled`, 0_UInt8), __table1.timestamp): While executing Remote. "
            "(NOT_FOUND_COLUMN_IN_BLOCK) (version 25.8.33.10002.altinitystable)",
            True,
            id="old-query-node",
        ),
        pytest.param(
            "Code: 10. DB::Exception: Not found column __table1.foo in block. "
            "There are only columns: __table1.bar: While executing Remote. "
            "(NOT_FOUND_COLUMN_IN_BLOCK)",
            False,
            id="different-column",
        ),
        pytest.param(
            "Code: 10. DB::Exception: Not found column `tags.key` in block. "
            "There are only columns: `tags.key`, x. (NOT_FOUND_COLUMN_IN_BLOCK)",
            False,
            id="not-remote",
        ),
    ],
)
def test_is_remote_header_mismatch(message: str, expected: bool) -> None:
    assert is_remote_header_mismatch(ClickhouseError(message, code=10)) is expected


def test_other_error_codes_do_not_match() -> None:
    message = (
        "Not found column arrayJoin(__table1.`tags.key`) in block. There are only "
        "columns: arrayJoin(__table1.tags.key): While executing Remote."
    )
    assert not is_remote_header_mismatch(ClickhouseError(message, code=47))
