import logging
from datetime import datetime
from typing import Optional, Sequence

from snuba.clickhouse.query import Query
from snuba.clickhouse.query_dsl.accessors import get_time_range
from snuba.clickhouse.translators.snuba.mappers import (
    KEY_COL_MAPPING_PARAM,
    VALUE_COL_MAPPING_PARAM,
    mapping_pattern,
)
from snuba.query.conditions import BooleanFunctions, ConditionFunctions
from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import Expression
from snuba.query.expressions import FunctionCall as FunctionCallExpr
from snuba.query.matchers import Any, Column, FunctionCall, Literal, Or, Param, String
from snuba.querylog.query_metadata import (
    ClickhouseQueryProfile,
    Columnset,
    FilterProfile,
)

logger = logging.getLogger(__name__)


def _get_date_range(query: Query) -> Optional[int]:
    """
    Best guess to find the time range for the query.
    We pick the first column that is compared with a datetime Literal.
    """
    pattern = FunctionCall(
        None,
        Or([String(ConditionFunctions.GT), String(ConditionFunctions.GTE)]),
        (Column(None, None, Param("col_name", Any(str))), Literal(None, Any(datetime))),
    )

    condition = query.get_condition_from_ast()
    if condition is None:
        return None
    for exp in condition:
        result = pattern.match(exp)
        if result is not None:
            from_date, to_date = get_time_range(query, result.string("col_name"))
            if from_date is None or to_date is None:
                return None
            else:
                return (to_date - from_date).days

    return None


def _get_table(query: Query) -> str:
    source = query.get_data_source()
    if source is None:
        # Should never happen at this point.
        return ""
    return source.format_from()


def _has_complex_conditions(query: Query) -> bool:
    condition = query.get_condition_from_ast()
    if condition is None:
        return False
    for c in condition:
        if isinstance(c, FunctionCallExpr) and c.function_name == BooleanFunctions.OR:
            return True
    return False


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

    try:
        return ClickhouseQueryProfile(
            time_range=_get_date_range(query),
            table=_get_table(query),
            all_columns=_get_all_columns(query),
            multi_level_condition=_has_complex_conditions(query),
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
