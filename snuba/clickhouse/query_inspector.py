from datetime import datetime
from typing import Optional, Set

from snuba.clickhouse.query_dsl.accessors import get_time_range
from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import BooleanFunctions, ConditionFunctions
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.simple import Table
from snuba.query.data_source.visitor import DataSourceVisitor
from snuba.query.expressions import FunctionCall as FunctionCallExpr
from snuba.query.matchers import Any, Column, FunctionCall, Literal, Or, Param, String


def _get_date_range(query: ProcessableQuery[Table]) -> Optional[int]:
    """
    Best guess to find the time range for the query.
    We pick the first column that is compared with a datetime Literal.
    """
    pattern = FunctionCall(
        Or([String(ConditionFunctions.GT), String(ConditionFunctions.GTE)]),
        (Column(None, Param("col_name", Any(str))), Literal(Any(datetime))),
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


class TablesCollector(DataSourceVisitor[None, Table], JoinVisitor[None, Table]):
    """
    Traverses the data source of a composite query and collects
    all the referenced table names, final state and sampling rate
    to fill stats.
    """

    def __init__(self) -> None:
        self.__tables: Set[str] = set()
        self.__max_time_range: Optional[int] = None
        self.__has_complex_conditions: bool = False
        self.__final: bool = False
        self.__sample_rate: Optional[float] = None

    def get_tables(self) -> Set[str]:
        return self.__tables

    def get_max_time_range(self) -> Optional[int]:
        return self.__max_time_range

    def has_complex_condition(self) -> bool:
        return self.__has_complex_conditions

    def any_final(self) -> bool:
        return self.__final

    def get_sample_rate(self) -> Optional[float]:
        return self.__sample_rate

    def __find_complex_conditions(self, query: ProcessableQuery[Table]) -> bool:
        condition = query.get_condition_from_ast()
        if condition is None:
            return False
        for c in condition:
            if (
                isinstance(c, FunctionCallExpr)
                and c.function_name == BooleanFunctions.OR
            ):
                return True
        return False

    def _visit_simple_source(self, data_source: Table) -> None:
        self.__tables.add(data_source.table_name)
        self.__sample_rate = data_source.sampling_rate
        if data_source.final:
            self.__final = True

    def _visit_join(self, data_source: JoinClause[Table]) -> None:
        self.visit_join_clause(data_source)

    def _visit_simple_query(self, data_source: ProcessableQuery[Table]) -> None:
        time_range = _get_date_range(data_source)
        if time_range and (
            self.__max_time_range is None or time_range > self.__max_time_range
        ):
            self.__max_time_range = time_range

        self.__has_complex_conditions = (
            self.__has_complex_conditions | self.__find_complex_conditions(data_source)
        )

        self.visit(data_source.get_from_clause())

    def _visit_composite_query(self, data_source: CompositeQuery[Table]) -> None:
        self.visit(data_source.get_from_clause())
        # stats do not yet support sampling rate (there is only one field)
        # so if we have a composite query we set it to None.
        self.__sample_rate = None

    def visit_individual_node(self, node: IndividualNode[Table]) -> None:
        self.visit(node.data_source)

    def visit_join_clause(self, node: JoinClause[Table]) -> None:
        node.left_node.accept(self)
        node.right_node.accept(self)
