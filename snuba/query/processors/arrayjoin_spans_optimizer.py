from typing import Callable, List, Optional, Sequence, Set, Tuple, TypeVar, Union

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
    combine_and_conditions,
    combine_or_conditions,
    get_first_level_and_conditions,
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


def op_column(col_name: str) -> str:
    return f"{col_name}.op"


def group_column(col_name: str) -> str:
    return f"{col_name}.group"


def val_column(col_name: str) -> str:
    return f"{col_name}.exclusive_time"


def array_join_pattern(column_name: str) -> FunctionCall:
    return FunctionCall(
        String("arrayJoin"), (Column(column_name=String(column_name)),),
    )


def array_join_tuple_pattern(column_name1: str, column_name2: str) -> FunctionCall:
    return FunctionCall(
        String("tuple"),
        (array_join_pattern(column_name1), array_join_pattern(column_name2)),
    )


def find_array_join(query: Query, column: str) -> bool:
    return any(
        array_join_pattern(column).match(f) is not None
        for selected in query.get_selected_columns() or []
        for f in selected.expression
    )


T = TypeVar("T", bound=Union[str, Tuple[str, str]])

Extractor = Callable[[Expression], Set[T]]


def _string_literal_equal_condition_extractor(
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


def _string_literal_in_condition_extractor(
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


def _tuple_literal_equal_condition_extractor(
    key_pattern: Pattern[Expression],
) -> Extractor[Tuple[str, str]]:
    def extractor(condition: Expression) -> Set[Tuple[str, str]]:
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

        parameters = [
            param.value
            for param in function.parameters
            if isinstance(param, LiteralExpr) and isinstance(param.value, str)
        ]

        if len(parameters) != 2:
            return set()

        return {(parameters[0], parameters[1])}

    return extractor


def _tuple_literal_in_condition_extractor(
    key_pattern: Pattern[Expression],
) -> Extractor[Tuple[str, str]]:
    def extractor(condition: Expression) -> Set[Tuple[str, str]]:
        match = is_in_condition_pattern(key_pattern).match(condition)

        if match is None:
            return set()

        function = match.expression("tuple")
        if (
            not isinstance(function, FunctionCallExpr)
            or function.function_name != "tuple"
        ):
            return set()

        parameters: Set[Tuple[str, str]] = set()

        for tuple_param in function.parameters:
            if (
                not isinstance(tuple_param, FunctionCallExpr)
                or tuple_param.function_name != "tuple"
                or len(tuple_param.parameters) != 2
            ):
                return set()

            params = [
                param.value
                for param in tuple_param.parameters
                if isinstance(param, LiteralExpr) and isinstance(param.value, str)
            ]

            if len(params) != 2:
                return set()

            parameters.add((params[0], params[1]))

        return parameters

    return extractor


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

        for search_value_extractor in extractors:
            keys_found |= search_value_extractor(c)

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


def get_filtered_mapping_keys_column(query: Query, column: str) -> Sequence[str]:
    pattern = array_join_pattern(column)
    return get_filtered_mapping_keys(
        query,
        [
            _string_literal_equal_condition_extractor(pattern),
            _string_literal_in_condition_extractor(pattern),
        ],
    )


def get_filtered_mapping_keys_tuple(
    query: Query, column1: str, column2: str
) -> Sequence[Tuple[str, str]]:
    pattern = array_join_tuple_pattern(column1, column2)
    return get_filtered_mapping_keys(
        query,
        [
            _tuple_literal_equal_condition_extractor(pattern),
            _tuple_literal_in_condition_extractor(pattern),
        ],
    )


class ArrayJoinSpansOptimizer(QueryProcessor):
    def __init__(self) -> None:
        self.__column_name = "spans"

    def __column_tuple_index(self, column: str) -> int:
        if column == op_column(self.__column_name):
            return 1
        if column == group_column(self.__column_name):
            return 2
        if column == val_column(self.__column_name):
            return 3
        raise ValueError(f"Unknown column: {column}")

    def __get_filtered_ops(
        self, query: Query, has_array_join_op: bool
    ) -> Sequence[str]:
        """
        Get the filtered values for the spans.op column.
        """
        if not has_array_join_op:
            return []

        column_name = op_column(self.__column_name)
        return get_filtered_mapping_keys_column(query, column_name)

    def __get_filtered_groups(
        self, query: Query, has_array_join_group: bool
    ) -> Sequence[str]:
        """
        Get the filtered values for the spans.group column.
        """
        if not has_array_join_group:
            return []

        column_name = group_column(self.__column_name)
        return get_filtered_mapping_keys_column(query, column_name)

    def __get_filtered_op_group_tuples(
        self, query: Query, has_array_join_op: bool, has_array_join_group: bool
    ) -> Sequence[Tuple[str, str]]:
        """
        Get the filtered values for the (spans.op, spans.group) tuple.
        """
        if not has_array_join_op or not has_array_join_group:
            return []

        return get_filtered_mapping_keys_tuple(
            query, op_column(self.__column_name), group_column(self.__column_name)
        )

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        arrayjoin_pattern = FunctionCall(
            String("arrayJoin"),
            (
                Column(
                    column_name=Param(
                        "col",
                        Or(
                            [
                                String(op_column(self.__column_name)),
                                String(group_column(self.__column_name)),
                                String(val_column(self.__column_name)),
                            ]
                        ),
                    ),
                ),
            ),
        )

        arrayjoins_in_query = set()
        for e in query.get_all_expressions():
            match = arrayjoin_pattern.match(e)
            if match is not None:
                arrayjoins_in_query.add(match.string("col"))

        # Looks to see if the array join on the op/group can be found in the selected columns
        has_array_join_op = find_array_join(query, op_column(self.__column_name))
        has_array_join_group = find_array_join(query, group_column(self.__column_name))

        filtered_ops: Sequence[str] = self.__get_filtered_ops(query, has_array_join_op)
        filtered_groups: Sequence[str] = self.__get_filtered_groups(
            query, has_array_join_group
        )
        filtered_op_group_tuples: Sequence[
            Tuple[str, str]
        ] = self.__get_filtered_op_group_tuples(
            query, has_array_join_op, has_array_join_group
        )

        # Ensures the alias we apply to the arrayJoin is not already taken.
        used_aliases = {exp.alias for exp in query.get_all_expressions()}
        triple_alias_root = f"snuba_all_{self.__column_name}"
        triple_alias = triple_alias_root
        index = 0
        while triple_alias in used_aliases:
            index += 1
            triple_alias = f"{triple_alias_root}_{index}"

        def replace_expression(expr: Expression) -> Expression:
            """
            Applies the appropriate optimization on a single arrayJoin expression.
            """
            match = arrayjoin_pattern.match(expr)
            if match is None:
                return expr

            if arrayjoins_in_query == {
                op_column(self.__column_name),
                group_column(self.__column_name),
                val_column(self.__column_name),
            }:
                # All of arrayJoin(spans.op), arrayJoin(spans.group), and
                # arrayJoin(spans.exclusive_time) expressions are present in the
                # query. Do the array join on all three columns together instead
                # of independently.

                array_index = LiteralExpr(
                    None, self.__column_tuple_index(match.string("col"))
                )

                if filtered_ops or filtered_groups or filtered_op_group_tuples:
                    # Filters on the ops, groups, and/or op, group tuple were found.
                    # Apply the arrayFilter optimization with them.
                    return _filtered_mapping_triples(
                        expr.alias,
                        self.__column_name,
                        triple_alias,
                        filtered_ops,
                        filtered_groups,
                        filtered_op_group_tuples,
                        array_index,
                    )
                else:
                    return _unfiltered_mapping_triples(
                        expr.alias, self.__column_name, triple_alias, array_index
                    )

            return expr

        query.transform_expressions(replace_expression)

        bloom_filter_condition = generate_bloom_filter_condition(
            self.__column_name, filtered_ops, filtered_groups, filtered_op_group_tuples
        )
        if bloom_filter_condition:
            query.add_condition_to_ast(bloom_filter_condition)


def _filtered_mapping_triples(
    alias: Optional[str],
    column_name: str,
    triple_alias: str,
    filtered_ops: Sequence[str],
    filtered_groups: Sequence[str],
    filtered_op_group_tuples: Sequence[Tuple[str, str]],
    tuple_index: LiteralExpr,
) -> Expression:
    # (arrayJoin(arrayFilter(
    #       triple -> tupleElement(triple, 1) IN (ops),
    #       arrayMap((x,y,z) -> (x,yz), spans.op, spans.group, spans.exclusive_time)
    #  )) as all_spans).1
    return tupleElement(
        alias,
        arrayJoin(
            triple_alias,
            filter_span_values(
                zip_columns(
                    ColumnExpr(None, None, op_column(column_name)),
                    ColumnExpr(None, None, group_column(column_name)),
                    ColumnExpr(None, None, val_column(column_name)),
                ),
                filtered_ops,
                filtered_groups,
                filtered_op_group_tuples,
            ),
        ),
        tuple_index,
    )


def _unfiltered_mapping_triples(
    alias: Optional[str], column_name: str, triple_alias: str, tuple_index: LiteralExpr
) -> Expression:
    # (arrayJoin(
    #   arrayMap((x,y,z) -> (x,yz), col.op, col.group col.val)
    #  as all_tags).1
    return tupleElement(
        alias,
        arrayJoin(
            triple_alias,
            zip_columns(
                ColumnExpr(None, None, op_column(column_name)),
                ColumnExpr(None, None, group_column(column_name)),
                ColumnExpr(None, None, val_column(column_name)),
            ),
        ),
        tuple_index,
    )


def filter_span_values(
    span_values: Expression,
    filtered_ops: Sequence[str],
    filtered_groups: Sequence[str],
    filtered_op_group_tuples: Sequence[Tuple[str, str]],
) -> Expression:

    conditions: List[Expression] = []

    argument_name = "triple"
    argument = Argument(None, argument_name)

    if filtered_ops:
        conditions.append(
            binary_condition(
                ConditionFunctions.IN,
                tupleElement(None, argument, LiteralExpr(None, 1)),
                FunctionCallExpr(
                    None, "tuple", tuple(LiteralExpr(None, op) for op in filtered_ops)
                ),
            )
        )

    if filtered_groups:
        conditions.append(
            binary_condition(
                ConditionFunctions.IN,
                tupleElement(None, argument, LiteralExpr(None, 2)),
                FunctionCallExpr(
                    None,
                    "tuple",
                    tuple(LiteralExpr(None, group) for group in filtered_groups),
                ),
            )
        )

    if filtered_op_group_tuples:
        conditions.append(
            binary_condition(
                ConditionFunctions.IN,
                FunctionCallExpr(
                    None,
                    "tuple",
                    (
                        tupleElement(None, argument, LiteralExpr(None, 1)),
                        tupleElement(None, argument, LiteralExpr(None, 2)),
                    ),
                ),
                FunctionCallExpr(
                    None,
                    "tuple",
                    tuple(
                        FunctionCallExpr(
                            None,
                            "tuple",
                            (LiteralExpr(None, op), LiteralExpr(None, group)),
                        )
                        for op, group in filtered_op_group_tuples
                    ),
                ),
            )
        )

    return FunctionCallExpr(
        None,
        "arrayFilter",
        (Lambda(None, ("triple",), combine_and_conditions(conditions)), span_values),
    )


def zip_columns(
    column1: ColumnExpr, column2: ColumnExpr, column3: ColumnExpr,
) -> Expression:
    """
    Turns 3 array columns into an array of triples
    """

    arguments = ("x", "y", "z")

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
            column1,
            column2,
            column3,
        ),
    )


def generate_bloom_filter_condition(
    column_name: str,
    filtered_ops: Sequence[str],
    filtered_groups: Sequence[str],
    filtered_op_group_tuples: Sequence[Tuple[str, str]],
) -> Optional[Expression]:
    """
    Generate the filters on the array columns to use the bloom filter index on
    the spans.op and spans.group columns in order to filter the transactions
    prior to the array join.

    The bloom filter index is requires the use of the has function, therefore
    the final condition is built up from a series of has conditions.
    """

    conditions: List[Expression] = []

    ops = set(filtered_ops)
    groups = set(filtered_groups)

    for op, group in filtered_op_group_tuples:
        ops.add(op)
        groups.add(group)

    op_conditions = [
        FunctionCallExpr(
            None,
            "has",
            (ColumnExpr(None, None, op_column(column_name)), LiteralExpr(None, op),),
        )
        for op in sorted(ops)
    ]

    group_conditions = [
        FunctionCallExpr(
            None,
            "has",
            (
                ColumnExpr(None, None, group_column(column_name)),
                LiteralExpr(None, group),
            ),
        )
        for group in sorted(groups)
    ]

    # OR the conditions on the ops together because we want all rows that have
    # at least one of the ops specified
    op_condition = combine_or_conditions(op_conditions) if op_conditions else None

    # same as above
    group_condition = (
        combine_or_conditions(group_conditions) if group_conditions else None
    )

    conditions = [
        condition for condition in [op_condition, group_condition] if condition
    ]
    return combine_and_conditions(conditions) if conditions else None
