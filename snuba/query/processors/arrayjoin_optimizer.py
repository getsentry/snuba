from collections import defaultdict
from itertools import combinations
from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple, TypeVar, Union

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
    combine_and_conditions,
    combine_or_conditions,
    get_first_level_and_conditions,
    in_condition,
    is_any_binary_condition,
    is_in_condition_pattern,
)
from snuba.query.dsl import arrayJoin, tupleElement
from snuba.query.expressions import Argument
from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import Expression
from snuba.query.expressions import FunctionCall as FunctionCallExpr
from snuba.query.expressions import Lambda
from snuba.query.expressions import Literal as LiteralExpr
from snuba.query.matchers import (
    Any,
    Column,
    FunctionCall,
    Literal,
    Or,
    Param,
    Pattern,
    String,
)
from snuba.request.request_settings import RequestSettings


class ArrayJoinOptimizer(QueryProcessor):
    def __init__(
        self,
        column_name: str,
        key_names: Sequence[str],
        val_names: Sequence[str],
        use_bf_index: Optional[bool] = False,
    ):
        assert key_names, "key_names cannot be empty"
        assert val_names, "val_names cannot be empty"

        self.__column_name = column_name
        self.__key_names = key_names
        self.__val_names = val_names
        self.__use_bf_index = use_bf_index

        self.__array_join_pattern: Optional[FunctionCall] = None

    @property
    def __key_columns(self) -> List[str]:
        """
        The full name of all the nested key columns
        """
        return [f"{self.__column_name}.{column}" for column in self.__key_names]

    @property
    def __val_column(self) -> List[str]:
        """
        The full name of all the nested value columns
        """
        return [f"{self.__column_name}.{column}" for column in self.__val_names]

    @property
    def __all_columns(self) -> List[str]:
        """
        The full name of all the nested columns
        """
        return self.__key_columns + self.__val_column

    @property
    def array_join_pattern(self) -> FunctionCall:
        """
        A matcher for an arrayJoin on any of the possible nested columns
        """
        if self.__array_join_pattern is None:
            self.__array_join_pattern = FunctionCall(
                String("arrayJoin"),
                (
                    Column(
                        column_name=Param(
                            "col",
                            Or([String(column) for column in self.__all_columns]),
                        ),
                    ),
                ),
            )

        return self.__array_join_pattern

    def __find_tuple_index(self, column_name: str) -> LiteralExpr:
        """
        Translates a column name to a tuple index. Used for accessing the specific
        column from within the single optimized arrayJoin.

        This should only be used when ALL possible columns are present in the select
        clause of the query because it assumes a specific ordering. If any of the
        possible columns are missing from the select clause, then the ordering will
        be not as expected.
        """
        for i, col in enumerate(self.__all_columns):
            if column_name == col:
                return LiteralExpr(None, i + 1)
        raise ValueError(f"Unknown column: {column_name}")

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        array_joins_in_query = self.__get_array_joins_in_query(query)

        tuple_alias = self.__get_unused_alias(query)

        single_filtered, multiple_filtered = self.__get_filtered(query)

        def replace_expression(expr: Expression) -> Expression:
            match = self.array_join_pattern.match(expr)

            # The arrayJoins we are looking for are not present, so skip this entirely
            if match is None:
                return expr

            # All of the possible array joins are present
            if array_joins_in_query == set(self.__all_columns):
                tuple_index = self.__find_tuple_index(match.string("col"))

                single_index_filtered = {
                    self.__find_tuple_index(column_name): filtered
                    for column_name, filtered in single_filtered.items()
                }

                multiple_indices_filtered = {
                    tuple(
                        self.__find_tuple_index(column) for column in column_names
                    ): filtered
                    for column_names, filtered in multiple_filtered.items()
                }

                if single_filtered or multiple_filtered:
                    return filtered_mapping_tuples(
                        expr.alias,
                        tuple_alias,
                        tuple_index,
                        self.__all_columns,
                        single_index_filtered,
                        multiple_indices_filtered,
                    )

                return unfiltered_mapping_tuples(
                    expr.alias, tuple_alias, tuple_index, self.__all_columns
                )

            # Only array join present is one of the key columns
            elif len(array_joins_in_query) == 1 and any(
                column in array_joins_in_query for column in self.__key_columns
            ):
                column_name = array_joins_in_query.pop()
                if column_name in single_filtered:
                    return filtered_mapping_keys(
                        expr.alias, column_name, single_filtered[column_name]
                    )

            # No viable optimization
            return expr

        query.transform_expressions(replace_expression)

        # If there is a bloom filter index present on the key columns, we want
        # to take advantage of it by adding in the necessary `has` conditions.
        if self.__use_bf_index:
            bloom_filter_condition = generate_bloom_filter_condition(
                self.__column_name, single_filtered, multiple_filtered
            )
            if bloom_filter_condition:
                query.add_condition_to_ast(bloom_filter_condition)

    def __get_array_joins_in_query(self, query: Query) -> Set[str]:
        """
        Get all of the arrayJoins on the possible columns that are present in the query.
        """
        array_joins_in_query: Set[str] = set()

        for e in query.get_all_expressions():
            match = self.array_join_pattern.match(e)
            if match is not None:
                array_joins_in_query.add(match.string("col"))

        return array_joins_in_query

    def __get_unused_alias(self, query: Query) -> str:
        """
        Get an unused alias to be used in the arrayJoin optimization.
        """
        used_aliases = {exp.alias for exp in query.get_all_expressions()}
        alias_root = f"snuba_all_{self.__column_name}"
        alias = alias_root
        index = 0

        while alias in used_aliases:
            index += 1
            alias = f"{alias_root}_{index}"

        return alias

    def __get_filtered(
        self, query: Query
    ) -> Tuple[
        Dict[str, Sequence[str]], Dict[Tuple[str, ...], Sequence[Tuple[str, ...]]]
    ]:
        """
        Get all the filters that were applied in the query. A filter may be applied
        to any of the individual key columns or even a tuple composed of multiple
        key columns.
        """
        # Check which array joins have been selected
        selected_array_joins = {
            column_name
            for column_name in self.__key_columns
            if find_pattern(query, array_join_pattern(column_name))
        }

        # Look for all the filters on a single array join
        single_filtered = {
            column_name: get_single_column_filters(query, column_name)
            for column_name in self.__key_columns
            if column_name in selected_array_joins
        }

        single_filtered = {
            column_name: filtered
            for column_name, filtered in single_filtered.items()
            if filtered
        }

        # Look for all the filters on a tuple of more than one array joins
        multiple_filtered = {
            tuple(column_names): get_multiple_columns_filters(query, column_names)
            for length in range(1, len(self.__key_columns) + 1)
            # NOTE: This only checks ONE permutation of the tuple, NOT ALL permutations.
            for column_names in combinations(self.__key_columns, length)
            if all(column_name in selected_array_joins for column_name in column_names)
        }

        multiple_filtered = {
            column_names: filtered
            for column_names, filtered in multiple_filtered.items()
            if filtered
        }

        return single_filtered, multiple_filtered


T = TypeVar("T", bound=Union[str, Tuple[str, ...]])
Extractor = Callable[[Expression], Set[T]]


def get_single_column_filters(query: Query, column_name: str) -> Sequence[str]:
    pattern = array_join_pattern(column_name)

    return get_filtered_mapping_keys(
        query,
        [
            string_literal_equal_condition_extractor(pattern),
            string_literal_in_condition_extractor(pattern),
        ],
    )


def get_multiple_columns_filters(
    query: Query, column_names: Tuple[str, ...]
) -> Sequence[Tuple[str, ...]]:
    pattern = array_join_pattern(*column_names)

    return get_filtered_mapping_keys(
        query,
        [
            tuple_literal_equal_condition_extractor(pattern),
            tuple_literal_in_condition_extractor(pattern),
        ],
    )


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
                zip_columns(
                    *[ColumnExpr(None, None, column) for column in column_names]
                ),
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
                            None, "tuple", tuple(LiteralExpr(None, t) for t in tuples),
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


def _array_join_pattern(column_name: str) -> FunctionCall:
    return FunctionCall(
        String("arrayJoin"), (Column(column_name=String(column_name)),),
    )


def array_join_pattern(*column_names: str) -> FunctionCall:
    if len(column_names) == 1:
        return _array_join_pattern(column_names[0])

    return FunctionCall(
        String("tuple"),
        tuple(_array_join_pattern(column_name) for column_name in column_names),
    )


def find_pattern(query: Query, pattern: FunctionCall) -> bool:
    return any(
        pattern.match(f) is not None
        for selected in query.get_selected_columns() or []
        for f in selected.expression
    )


def get_mapping_keys_in_condition(
    conditions: Expression, extractors: Sequence[Extractor[T]]
) -> Optional[Set[T]]:
    """
    Examines the top level AND conditions and applies the extractor functions to
    extract the matching keys.

    If any we find any OR conditions, we exit early though there could be possible
    optimizations to be done in these situations.
    """
    keys_found: Set[T] = set()

    for c in get_first_level_and_conditions(conditions):
        if is_any_binary_condition(c, BooleanFunctions.OR):
            return None

        for extractor in extractors:
            keys_found |= extractor(c)

    return keys_found


def get_filtered_mapping_keys(
    query: Query, extractors: Sequence[Extractor[T]]
) -> Sequence[T]:
    """
    Identifies the conditions we can apply the arrayFilter optimization on.

    Which means: if the arrayJoin is in the select clause, there are one or
    more top level AND condition on the arrayJoin and there is no OR condition
    in the query.
    """
    ast_condition = query.get_condition()
    cond_keys: Optional[Set[T]] = (
        get_mapping_keys_in_condition(ast_condition, extractors)
        if ast_condition is not None
        else set()
    )
    if cond_keys is None:
        # This means we found an OR. Cowardly we give up even though there could
        # be cases where this condition is still optimizable.
        return []

    ast_having = query.get_having()
    having_keys: Optional[Set[T]] = (
        get_mapping_keys_in_condition(ast_having, extractors)
        if ast_having is not None
        else set()
    )
    if having_keys is None:
        # Same as above
        return []

    keys = cond_keys | having_keys
    return sorted(list(keys))


def string_literal_equal_condition_extractor(
    key_pattern: Pattern[Expression],
) -> Extractor[str]:
    def extractor(condition: Expression) -> Set[str]:
        match = FunctionCall(
            String(ConditionFunctions.EQ),
            (key_pattern, Literal(Param("key", Any(str)))),
        ).match(condition)

        if match is None:
            return set()

        return {match.string("key")}

    return extractor


def string_literal_in_condition_extractor(
    key_pattern: Pattern[Expression],
) -> Extractor[str]:
    def extractor(condition: Expression) -> Set[str]:
        match = is_in_condition_pattern(key_pattern).match(condition)

        if match is None:
            return set()

        function = match.expression("tuple")
        assert isinstance(function, FunctionCallExpr)

        return {
            param.value
            for param in function.parameters
            if isinstance(param, LiteralExpr) and isinstance(param.value, str)
        }

    return extractor


def tuple_literal_equal_condition_extractor(
    key_pattern: Pattern[Expression],
) -> Extractor[Tuple[str, ...]]:
    def extractor(condition: Expression) -> Set[Tuple[str, ...]]:
        match = FunctionCall(
            String(ConditionFunctions.EQ),
            (key_pattern, Param("tuple", FunctionCall(String("tuple"), None))),
        ).match(condition)

        if match is None:
            return set()

        function = match.expression("tuple")
        if (
            not isinstance(function, FunctionCallExpr)
            or function.function_name != "tuple"
        ):
            return set()

        parameters = tuple(
            param.value
            for param in function.parameters
            if isinstance(param, LiteralExpr) and isinstance(param.value, str)
        )

        return {parameters}

    return extractor


def tuple_literal_in_condition_extractor(
    key_pattern: Pattern[Expression],
) -> Extractor[Tuple[str, ...]]:
    def extractor(condition: Expression) -> Set[Tuple[str, ...]]:
        match = is_in_condition_pattern(key_pattern).match(condition)

        if match is None:
            return set()

        function = match.expression("tuple")
        if (
            not isinstance(function, FunctionCallExpr)
            or function.function_name != "tuple"
        ):
            return set()

        parameters: Set[Tuple[str, ...]] = set()

        for tuple_param in function.parameters:
            if (
                not isinstance(tuple_param, FunctionCallExpr)
                or tuple_param.function_name != "tuple"
            ):
                return set()

            parameters.add(
                tuple(
                    param.value
                    for param in tuple_param.parameters
                    if isinstance(param, LiteralExpr) and isinstance(param.value, str)
                )
            )

        return parameters

    return extractor


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
                FunctionCallExpr(
                    None, "tuple", tuple(Argument(None, arg) for arg in arguments)
                ),
            ),
            *columns,
        ),
    )


def generate_bloom_filter_condition(
    column_name: str,
    single_filtered: Dict[str, Sequence[str]],
    multiple_filtered: Dict[Tuple[str, ...], Sequence[Tuple[str, ...]]],
) -> Optional[Expression]:
    """
    Generate the filters on the array columns to use the bloom filter index on
    the spans.op and spans.group columns in order to filter the transactions
    prior to the array join.

    The bloom filter index is requires the use of the has function, therefore
    the final condition is built up from a series of has conditions.
    """

    per_key_vals: Dict[str, Set[str]] = defaultdict(set)

    for key, single_filter in single_filtered.items():
        for val in single_filter:
            per_key_vals[key].add(val)

    for keys, multiple_filter in multiple_filtered.items():
        for val_tuple in multiple_filter:
            for key, val in zip(keys, val_tuple):
                per_key_vals[key].add(val)

    per_key_conditions: Dict[str, List[Expression]] = defaultdict(list)

    for key, vals in per_key_vals.items():
        for val in sorted(vals):
            per_key_conditions[key].append(
                FunctionCallExpr(
                    None, "has", (ColumnExpr(None, None, key), LiteralExpr(None, val),),
                )
            )

    all_conditions: List[Expression] = [
        combine_or_conditions(conditions)
        for conditions in per_key_conditions.values()
        if conditions
    ]

    return combine_and_conditions(all_conditions) if all_conditions else None
