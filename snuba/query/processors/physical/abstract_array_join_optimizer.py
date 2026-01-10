from itertools import combinations
from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple, TypeVar, Union

from snuba.clickhouse.query import Query
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    get_first_level_and_conditions,
    get_first_level_or_conditions,
    is_any_binary_condition,
    is_in_condition_pattern,
)
from snuba.query.expressions import Expression
from snuba.query.expressions import FunctionCall as FunctionCallExpr
from snuba.query.expressions import Literal as LiteralExpr
from snuba.query.matchers import (
    Any,
    Column,
    FunctionCall,
    Literal,
    Param,
    Pattern,
    String,
)
from snuba.query.processors.physical import ClickhouseQueryProcessor


class AbstractArrayJoinOptimizer(ClickhouseQueryProcessor):
    def __init__(
        self,
        column_name: str,
        key_names: Sequence[str],
        val_names: Sequence[str],
    ):
        assert key_names, "key_names cannot be empty"
        assert val_names, "val_names cannot be empty"

        self.column_name = column_name
        self.__key_names = key_names
        self.__val_names = val_names

    @property
    def key_columns(self) -> List[str]:
        """
        The full name of all the nested key columns
        """
        return [f"{self.column_name}.{column}" for column in self.__key_names]

    @property
    def val_column(self) -> List[str]:
        """
        The full name of all the nested value columns
        """
        return [f"{self.column_name}.{column}" for column in self.__val_names]

    @property
    def all_columns(self) -> List[str]:
        """
        The full name of all the nested columns
        """
        return self.key_columns + self.val_column

    def get_filtered_arrays(
        self, query: Query, all_column_names: Sequence[str]
    ) -> Tuple[Dict[str, Sequence[str]], Dict[Tuple[str, ...], Sequence[Tuple[str, ...]]]]:
        # Check which array joins have been selected
        selected_array_joins = {
            column_name
            for column_name in all_column_names
            if find_pattern(query, array_join_pattern(column_name))
        }

        # Look for all the filters on a single array join
        single_filtered = {
            column_name: get_single_column_filters(query, column_name)
            for column_name in all_column_names
            if column_name in selected_array_joins
        }

        single_filtered = {
            column_name: filtered for column_name, filtered in single_filtered.items() if filtered
        }

        # Look for all the filters on a tuple of more than one array joins
        multiple_filtered = {
            tuple(column_names): get_multiple_columns_filters(query, column_names)
            for length in range(2, len(all_column_names) + 1)
            # NOTE: This only checks ONE permutation of the tuple, NOT ALL permutations.
            for column_names in combinations(all_column_names, length)
            if all(column_name in selected_array_joins for column_name in column_names)
        }

        multiple_filtered = {
            column_names: filtered
            for column_names, filtered in multiple_filtered.items()
            if filtered
        }

        return single_filtered, multiple_filtered


def find_pattern(query: Query, pattern: FunctionCall) -> bool:
    return any(
        pattern.match(f) is not None
        for selected in query.get_selected_columns() or []
        for f in selected.expression
    )


def _array_join_pattern(column_name: str) -> FunctionCall:
    return FunctionCall(
        String("arrayJoin"),
        (Column(column_name=String(column_name)),),
    )


def array_join_pattern(*column_names: str) -> FunctionCall:
    if len(column_names) == 1:
        return _array_join_pattern(column_names[0])

    return FunctionCall(
        String("tuple"),
        tuple(_array_join_pattern(column_name) for column_name in column_names),
    )


T = TypeVar("T", bound=Union[str, Tuple[str, ...]])
Extractor = Callable[[Expression], Set[T]]


def skippable_condition_pattern(*column_names: str) -> Callable[[Expression], bool]:
    def is_skippable_condition(conditions: Expression) -> bool:
        """
        A condition composed of a bunch of has(column, ...) conditions OR'ed together
        can be ignored when looking for filter keys because these are the conditions
        used for the bloom filter index on the array column.
        """
        for column_name in column_names:
            has_pattern = FunctionCall(
                String("has"),
                (Column(column_name=String(column_name)), Literal(Any(str))),
            )
            if all(has_pattern.match(c) for c in get_first_level_or_conditions(conditions)):
                return True
        return False

    return is_skippable_condition


def get_single_column_filters(query: Query, column_name: str) -> Sequence[str]:
    pattern = array_join_pattern(column_name)

    return get_filtered_mapping_keys(
        query,
        [
            string_literal_equal_condition_extractor(pattern),
            string_literal_in_condition_extractor(pattern),
        ],
        skippable_condition_pattern(column_name),
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
        skippable_condition_pattern(*column_names),
    )


def get_filtered_mapping_keys(
    query: Query,
    extractors: Sequence[Extractor[T]],
    is_skippable_condition: Callable[[Expression], bool],
) -> Sequence[T]:
    """
    Identifies the conditions we can apply the arrayFilter optimization on.

    Which means: if the arrayJoin is in the select clause, there are one or
    more top level AND condition on the arrayJoin and there is no OR condition
    in the query.
    """
    ast_condition = query.get_condition()
    cond_keys: Optional[Set[T]] = (
        get_mapping_keys_in_condition(ast_condition, extractors, is_skippable_condition)
        if ast_condition is not None
        else set()
    )
    if cond_keys is None:
        # This means we found an OR. Cowardly we give up even though there could
        # be cases where this condition is still optimizable.
        return []

    ast_having = query.get_having()
    having_keys: Optional[Set[T]] = (
        get_mapping_keys_in_condition(ast_having, extractors, is_skippable_condition)
        if ast_having is not None
        else set()
    )
    if having_keys is None:
        # Same as above
        return []

    keys = cond_keys | having_keys
    return sorted(list(keys))


def get_mapping_keys_in_condition(
    conditions: Expression,
    extractors: Sequence[Extractor[T]],
    is_skippable_condition: Callable[[Expression], bool],
) -> Optional[Set[T]]:
    """
    Examines the top level AND conditions and applies the extractor functions to
    extract the matching keys.

    If any we find any OR conditions, we exit early though there could be possible
    optimizations to be done in these situations.
    """
    keys_found: Set[T] = set()

    for c in get_first_level_and_conditions(conditions):
        if is_skippable_condition(c):
            continue

        if is_any_binary_condition(c, BooleanFunctions.OR):
            return None

        for extractor in extractors:
            keys_found |= extractor(c)

    return keys_found


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

        function = match.expression("sequence")
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
        if not isinstance(function, FunctionCallExpr) or function.function_name != "tuple":
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

        function = match.expression("sequence")
        if not isinstance(function, FunctionCallExpr) or function.function_name not in (
            "tuple",
            "array",
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
