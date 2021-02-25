import logging
from typing import Iterable, List, Mapping, Set, Union

from snuba.clickhouse.query import Query
from snuba.clickhouse.query_inspector import TablesCollector
from snuba.clickhouse.translators.snuba.mappers import (
    KEY_COL_MAPPING_PARAM,
    VALUE_COL_MAPPING_PARAM,
    mapping_pattern,
)
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import Expression
from snuba.querylog.query_metadata import (
    ClickhouseQueryProfile,
    Columnset,
    FilterProfile,
)

logger = logging.getLogger(__name__)


def _get_all_columns(all_columns: Mapping[str, Set[ColumnExpr]]) -> Columnset:
    return {
        f"{table_name}.{c.column_name}"
        for table_name, columns in all_columns.items()
        for c in columns
    }


def _get_columns_from_expression(expression: Expression, table_name: str) -> Columnset:
    return {
        f"{table_name}.{c.column_name}" for c in expression if isinstance(c, ColumnExpr)
    }


def _list_columns(expressions: Mapping[str, Set[Expression]]) -> Columnset:
    ret = set()
    for table_name, expression_set in expressions.items():
        for e in expression_set:
            ret |= _get_columns_from_expression(e, table_name)

    return ret


def _flatten_col_set(nested_sets: Iterable[Set[str]]) -> Columnset:
    ret = set()
    for s in nested_sets:
        ret |= s
    return ret


def _list_columns_in_condition(
    condition_expression: Mapping[str, Expression]
) -> Columnset:
    return _flatten_col_set(
        [
            {c for c in _get_columns_from_expression(expression, table_name)}
            for table_name, expression in condition_expression.items()
        ]
    )


def _list_mappings(condition_expression: Mapping[str, Expression]) -> Columnset:
    nested_sets: List[Set[str]] = []
    for table_name, expression in condition_expression.items():
        ret = set()
        for e in expression:
            result = mapping_pattern.match(e)
            if result is not None:
                ret |= {
                    f"{table_name}.{result.string(KEY_COL_MAPPING_PARAM)}",
                    f"{table_name}.{result.string(VALUE_COL_MAPPING_PARAM)}",
                }
        nested_sets.append(ret)
    return _flatten_col_set(nested_sets)


def generate_profile(
    query: Union[Query, CompositeQuery[Table]]
) -> ClickhouseQueryProfile:
    """
    Takes a Physical query in, analyzes it and produces the
    ClickhouseQueryProfile data structure.
    """
    collector = TablesCollector()
    collector.visit(query)

    all_condition = collector.get_all_conditions()

    try:
        return ClickhouseQueryProfile(
            time_range=collector.get_max_time_range(),
            table=",".join(sorted([t for t in collector.get_tables()])),
            all_columns=_get_all_columns(collector.get_all_raw_columns()),
            multi_level_condition=collector.has_complex_condition(),
            where_profile=FilterProfile(
                columns=_list_columns_in_condition(all_condition),
                mapping_cols=_list_mappings(all_condition),
            ),
            groupby_cols=_list_columns(collector.get_all_groupby()),
            array_join_cols=_list_columns(collector.get_all_arrayjoin()),
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
