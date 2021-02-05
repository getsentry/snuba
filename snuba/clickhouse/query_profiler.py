import logging
from typing import Sequence

from snuba.clickhouse.query import Query
from snuba.clickhouse.query_inspector import TablesCollector
from snuba.clickhouse.translators.snuba.mappers import (
    KEY_COL_MAPPING_PARAM,
    VALUE_COL_MAPPING_PARAM,
    mapping_pattern,
)
from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import Expression
from snuba.query.expressions import FunctionCall as FunctionCallExpr
from snuba.querylog.query_metadata import (
    ClickhouseQueryProfile,
    Columnset,
    FilterProfile,
)

logger = logging.getLogger(__name__)


def _get_all_columns(query: Query) -> Columnset:
    return {c.column_name for c in query.get_all_ast_referenced_columns()}


def _get_columns_from_expression(expression: Expression) -> Columnset:
    return {c.column_name for c in expression if isinstance(c, ColumnExpr)}


def _list_columns(filter_expression: Expression) -> Columnset:
    return _get_columns_from_expression(filter_expression)


def _list_mapping(filter_expression: Expression) -> Columnset:
    ret = set()
    for e in filter_expression:
        result = mapping_pattern.match(e)
        if result is not None:
            ret |= {
                result.string(KEY_COL_MAPPING_PARAM),
                result.string(VALUE_COL_MAPPING_PARAM),
            }
    return ret


def _list_array_join(query: Query) -> Columnset:
    ret = set()
    query_arrayjoin = query.get_arrayjoin_from_ast()
    if query_arrayjoin is not None:
        ret |= _get_columns_from_expression(query_arrayjoin)

    for e in query.get_all_expressions():
        if isinstance(e, FunctionCallExpr) and e.function_name == "arrayJoin":
            ret |= _get_columns_from_expression(e)

    return ret


def _list_groupby_columns(groupby: Sequence[Expression]) -> Columnset:
    ret = set()
    for group in groupby:
        ret |= _get_columns_from_expression(group)
    return ret


def generate_profile(query: Query) -> ClickhouseQueryProfile:
    """
    Takes a Physical query in, analyzes it and produces the
    ClickhouseQueryProfile data structure.
    """
    where = query.get_condition_from_ast()
    groupby = query.get_groupby_from_ast()

    collector = TablesCollector()
    collector.visit(query)

    try:
        return ClickhouseQueryProfile(
            time_range=collector.get_max_time_range(),
            table=",".join(sorted([t for t in collector.get_tables()])),
            all_columns=_get_all_columns(query),
            multi_level_condition=collector.has_complex_condition(),
            where_profile=FilterProfile(
                columns=_list_columns(where) if where is not None else set(),
                mapping_cols=_list_mapping(where) if where is not None else set(),
            ),
            groupby_cols=_list_groupby_columns(groupby)
            if groupby is not None
            else set(),
            array_join_cols=_list_array_join(query),
        )
    except Exception:
        # Should never happen, but it is not worth failing queries while
        # rolling this out because we cannot build he profile.
        logger.warning("Failed to build query profile", exc_info=True)
        return ClickhouseQueryProfile(
            time_range=-1,
            table="",
            all_columns=set(),
            multi_level_condition=False,
            where_profile=FilterProfile(columns=set(), mapping_cols=set(),),
            groupby_cols=set(),
            array_join_cols=set(),
        )
