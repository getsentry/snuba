from typing import Dict, List, Optional, Sequence, Set, Tuple

from snuba.clickhouse.query import Query
from snuba.query.conditions import (
    ConditionFunctions,
    binary_condition,
    combine_and_conditions,
    in_condition,
)
from snuba.query.dsl import arrayJoin, tupleElement
from snuba.query.expressions import Argument, Expression, Lambda
from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import FunctionCall as FunctionCallExpr
from snuba.query.expressions import Literal as LiteralExpr
from snuba.query.matchers import Column, FunctionCall, Or, Param, String
from snuba.query.processors.physical.abstract_array_join_optimizer import (
    AbstractArrayJoinOptimizer,
)
from snuba.query.query_settings import QuerySettings


class ArrayJoinOptimizer(AbstractArrayJoinOptimizer):
    def __init__(
        self,
        column_name: str,
        key_names: Sequence[str],
        val_names: Sequence[str],
    ):
        super().__init__(column_name, key_names, val_names)
        self.__array_join_pattern = FunctionCall(
            String("arrayJoin"),
            (
                Column(
                    column_name=Param(
                        "col",
                        Or([String(column) for column in self.all_columns]),
                    ),
                ),
            ),
        )

    def __find_tuple_index(self, column_name: str) -> LiteralExpr:
        """
        Translates a column name to a tuple index. Used for accessing the specific
        column from within the single optimized arrayJoin.

        This should only be used when ALL possible columns are present in the select
        clause of the query because it assumes a specific ordering. If any of the
        possible columns are missing from the select clause, then the ordering will
        be not as expected.
        """
        for i, col in enumerate(self.all_columns):
            if column_name == col:
                return LiteralExpr(None, i + 1)
        raise ValueError(f"Unknown column: {column_name}")

    def __get_array_joins_in_query(self, query: Query) -> Set[str]:
        """
        Get all of the arrayJoins on the possible columns that are present in the query.
        """
        array_joins_in_query: Set[str] = set()

        for e in query.get_all_expressions():
            match = self.__array_join_pattern.match(e)
            if match is not None:
                array_joins_in_query.add(match.string("col"))

        return array_joins_in_query

    def __get_unused_alias(self, query: Query) -> str:
        """
        Get an unused alias to be used in the arrayJoin optimization.
        """
        used_aliases = {exp.alias for exp in query.get_all_expressions()}
        alias_root = f"snuba_all_{self.column_name}"
        alias = alias_root
        index = 0

        while alias in used_aliases:
            index += 1
            alias = f"{alias_root}_{index}"

        return alias

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        array_joins_in_query = self.__get_array_joins_in_query(query)

        tuple_alias = self.__get_unused_alias(query)

        single_filtered, multiple_filtered = self.get_filtered_arrays(query, self.key_columns)

        def replace_expression(expr: Expression) -> Expression:
            match = self.__array_join_pattern.match(expr)

            # The arrayJoins we are looking for are not present, so skip this entirely
            if match is None:
                return expr

            # All of the possible array joins are present
            if array_joins_in_query == set(self.all_columns):
                tuple_index = self.__find_tuple_index(match.string("col"))

                single_index_filtered = {
                    self.__find_tuple_index(column_name): filtered
                    for column_name, filtered in single_filtered.items()
                }

                multiple_indices_filtered = {
                    tuple(self.__find_tuple_index(column) for column in column_names): filtered
                    for column_names, filtered in multiple_filtered.items()
                }

                if single_filtered or multiple_filtered:
                    return filtered_mapping_tuples(
                        expr.alias,
                        tuple_alias,
                        tuple_index,
                        self.all_columns,
                        single_index_filtered,
                        multiple_indices_filtered,
                    )

                return unfiltered_mapping_tuples(
                    expr.alias, tuple_alias, tuple_index, self.all_columns
                )

            # Only array join present is one of the key columns
            elif len(array_joins_in_query) == 1 and any(
                column in array_joins_in_query for column in self.key_columns
            ):
                column_name = array_joins_in_query.pop()
                if column_name in single_filtered:
                    return filtered_mapping_keys(
                        expr.alias, column_name, single_filtered[column_name]
                    )

            # No viable optimization
            return expr

        query.transform_expressions(replace_expression)


def filtered_mapping_tuples(
    alias: Optional[str],
    tuple_alias: str,
    tuple_index: LiteralExpr,
    column_names: Sequence[str],
    single_filtered: Dict[LiteralExpr, Sequence[str]],
    multiple_filtered: Dict[Tuple[LiteralExpr, ...], Sequence[Tuple[str, ...]]],
) -> Expression:
    return tupleElement(
        alias,
        arrayJoin(
            tuple_alias,
            filter_expression(
                zip_columns(*[ColumnExpr(None, None, column) for column in column_names]),
                single_filtered,
                multiple_filtered,
            ),
        ),
        tuple_index,
    )


def filter_expression(
    columns: Expression,
    single_filtered: Dict[LiteralExpr, Sequence[str]],
    multiple_filtered: Dict[Tuple[LiteralExpr, ...], Sequence[Tuple[str, ...]]],
) -> Expression:
    argument_name = "arg"
    argument = Argument(None, argument_name)

    conditions: List[Expression] = []

    for index in single_filtered:
        conditions.append(
            binary_condition(
                ConditionFunctions.IN,
                tupleElement(None, argument, index),
                FunctionCallExpr(
                    None,
                    "tuple",
                    tuple(LiteralExpr(None, f) for f in single_filtered[index]),
                ),
            )
        )

    for indices in multiple_filtered:
        conditions.append(
            binary_condition(
                ConditionFunctions.IN,
                FunctionCallExpr(
                    None,
                    "tuple",
                    tuple(tupleElement(None, argument, index) for index in indices),
                ),
                FunctionCallExpr(
                    None,
                    "tuple",
                    tuple(
                        FunctionCallExpr(
                            None,
                            "tuple",
                            tuple(LiteralExpr(None, t) for t in tuples),
                        )
                        for tuples in multiple_filtered[indices]
                    ),
                ),
            )
        )

    return FunctionCallExpr(
        None,
        "arrayFilter",
        (Lambda(None, (argument_name,), combine_and_conditions(conditions)), columns),
    )


def unfiltered_mapping_tuples(
    alias: Optional[str],
    tuple_alias: str,
    tuple_index: LiteralExpr,
    column_names: Sequence[str],
) -> Expression:
    return tupleElement(
        alias,
        arrayJoin(
            tuple_alias,
            zip_columns(*[ColumnExpr(None, None, column) for column in column_names]),
        ),
        tuple_index,
    )


def filtered_mapping_keys(
    alias: Optional[str], column_name: str, filtered: Sequence[str]
) -> Expression:
    return arrayJoin(
        alias,
        filter_column(
            ColumnExpr(None, None, column_name),
            [LiteralExpr(None, f) for f in filtered],
        ),
    )


def filter_column(column: Expression, keys: Sequence[LiteralExpr]) -> Expression:
    return FunctionCallExpr(
        None,
        "arrayFilter",
        (Lambda(None, ("x",), in_condition(Argument(None, "x"), keys)), column),
    )


def zip_columns(*columns: ColumnExpr) -> Expression:
    if len(columns) not in {2, 3}:
        raise NotImplementedError("Can only zip between 2 and 3 columns.")

    arguments = ("x", "y", "z")[: len(columns)]

    return FunctionCallExpr(
        None,
        "arrayMap",
        (
            Lambda(
                None,
                arguments,
                FunctionCallExpr(None, "tuple", tuple(Argument(None, arg) for arg in arguments)),
            ),
            *columns,
        ),
    )
