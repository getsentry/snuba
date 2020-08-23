from typing import Optional, Sequence, Set

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    get_first_level_and_conditions,
    in_condition,
    is_binary_condition,
    is_in_condition_pattern,
)
from snuba.query.dsl import arrayJoin, tupleElement
from snuba.query.expressions import Argument
from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import Expression
from snuba.query.expressions import FunctionCall as FunctionCallExpr
from snuba.query.expressions import Lambda
from snuba.query.expressions import Literal as LiteralExpr
from snuba.query.matchers import Any, Column, FunctionCall, Literal, Or, Param, String
from snuba.request.request_settings import RequestSettings


def key_column(col_name: str) -> str:
    return f"{col_name}.key"


def val_column(col_name: str) -> str:
    return f"{col_name}.value"


def array_join_pattern(column_name: str) -> FunctionCall:
    return FunctionCall(
        None,
        String("arrayJoin"),
        (Column(column_name=String(key_column(column_name))),),
    )


def _get_mapping_keys_in_condition(
    condition: Expression, column_name: str
) -> Optional[Set[str]]:
    """
    Finds the top level conditions that include filter based on the arrayJoin.
    This is meant to be used to find the keys the query is filtering the arrayJoin
    on.
    We can only apply the arrayFilter optimization to arrayJoin conditions
    that are not in OR with other columns. To simplify the problem, we only
    consider those conditions that are included in the first level of the query:
    [['tagskey' '=' 'a'],['col' '=' 'b'],['col2' '=' 'c']]  works
    [[['tagskey' '=' 'a'], ['col2' '=' 'b']], ['tagskey' '=' 'c']] does not

    If we encounter an OR condition we return None, which means we cannot
    safely apply the optimization. Empty set means we did not find any
    suitable arrayJoin for optimization in this condition but that does
    not disqualify the whole query in the way the OR condition does.
    """
    keys_found = set()

    conditions = get_first_level_and_conditions(condition)
    for c in conditions:
        if is_binary_condition(c, BooleanFunctions.OR):
            return None

        match = FunctionCall(
            None,
            String(ConditionFunctions.EQ),
            (array_join_pattern(column_name), Literal(None, Param("key", Any(str)))),
        ).match(c)
        if match is not None:
            keys_found.add(match.string("key"))

        match = is_in_condition_pattern(array_join_pattern(column_name)).match(c)
        if match is not None:
            function = match.expression("tuple")
            assert isinstance(function, FunctionCallExpr)
            keys_found |= {
                l.value
                for l in function.parameters
                if isinstance(l, LiteralExpr) and isinstance(l.value, str)
            }

    return keys_found


def get_filtered_mapping_keys(query: Query, column_name: str) -> Set[str]:
    """
    Identifies the conditions we can apply the arrayFilter optimization
    on.
    Which means: if the arrayJoin is in the select clause, there
    are one or more top level AND condition on the arrayJoin and
    there is no OR condition in the query.
    """
    array_join_found = any(
        array_join_pattern(column_name).match(f) is not None
        for selected in query.get_selected_columns_from_ast() or []
        for f in selected.expression
    )

    if not array_join_found:
        return set()

    ast_condition = query.get_condition_from_ast()
    cond_keys = (
        _get_mapping_keys_in_condition(ast_condition, column_name)
        if ast_condition is not None
        else set()
    )
    if cond_keys is None:
        # This means we found an OR. Cowardly we give up even though there could
        # be cases where this condition is still optimizable.
        return set()

    ast_having = query.get_having_from_ast()
    having_keys = (
        _get_mapping_keys_in_condition(ast_having, column_name)
        if ast_having is not None
        else set()
    )
    if having_keys is None:
        # Same as above
        return set()

    return cond_keys | having_keys


class ArrayJoinKeyValueOptimizer(QueryProcessor):
    """
    Applies two optimizations to reduce the performance impact of
    arrayJoin operations performed on mapping columns.

    - it performs the arrayJoin only once in case both key and
        value are present for the same column in the query by
        arrayJoining on the key, value pairs instead of arrayJoining
        twice, once on keys and the second on values.
    - prefilter the keys with arrayFilter inside the arrayJoin if there
        are conditions in the query that specify which keys we want
        instead of doing the entire arrayJoin and filter the results.
    """

    def __init__(self, column_name: str) -> None:
        self.__column_name = column_name

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        arrayjoin_pattern = FunctionCall(
            None,
            String("arrayJoin"),
            (
                Column(
                    column_name=Param(
                        "col",
                        Or(
                            [
                                String(key_column(self.__column_name)),
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

        filtered_keys = [
            LiteralExpr(None, key)
            for key in get_filtered_mapping_keys(query, self.__column_name)
        ]

        # Ensures the alias we apply to the arrayJoin is not already taken.
        used_aliases = {exp.alias for exp in query.get_all_expressions()}
        pair_alias_root = f"snuba_all_{self.__column_name}"
        pair_alias = pair_alias_root
        index = 0
        while pair_alias in used_aliases:
            index += 1
            pair_alias = f"{pair_alias_root}_{index}"

        def replace_expression(expr: Expression) -> Expression:
            """
            Applies the appropriate optimization on a single arrayJoin expression.
            """
            match = arrayjoin_pattern.match(expr)
            if match is None:
                return expr

            if arrayjoins_in_query == {
                key_column(self.__column_name),
                val_column(self.__column_name),
            }:
                # Both arrayJoin(col.key) and arrayJoin(col.value) expressions
                # present int the query. Do the arrayJoin on key-value pairs
                # instead of independent arrayjoin for keys and values.
                array_index = (
                    LiteralExpr(None, 1)
                    if match.string("col") == key_column(self.__column_name)
                    else LiteralExpr(None, 2)
                )

                if not filtered_keys:
                    return _unfiltered_mapping_pairs(
                        expr.alias, self.__column_name, pair_alias, array_index
                    )
                else:
                    return _filtered_mapping_pairs(
                        expr.alias,
                        self.__column_name,
                        pair_alias,
                        filtered_keys,
                        array_index,
                    )

            elif filtered_keys:
                # Only one between arrayJoin(col.key) and arrayJoin(col.value)
                # is present, and it is arrayJoin(col.key) since we found
                # filtered keys.
                return _filtered_mapping_keys(
                    expr.alias, self.__column_name, filtered_keys
                )
            else:
                # No viable optimization
                return expr

        query.transform_expressions(replace_expression)


def _unfiltered_mapping_pairs(
    alias: Optional[str], column_name: str, pair_alias: str, array_index: LiteralExpr
) -> Expression:
    # (arrayJoin(
    #   arrayMap((x,y) -> [x,y], tags.key, tags.value)
    #  as all_tags)[1]
    return tupleElement(
        alias,
        arrayJoin(
            pair_alias,
            zip_columns(
                ColumnExpr(None, None, key_column(column_name)),
                ColumnExpr(None, None, val_column(column_name)),
            ),
        ),
        array_index,
    )


def _filtered_mapping_pairs(
    alias: Optional[str],
    column_name: str,
    pair_alias: str,
    filtered_tags: Sequence[LiteralExpr],
    array_index: LiteralExpr,
) -> Expression:
    # (arrayJoin(arrayFilter(
    #       pair -> tupleElement(pair, 1) IN (tags),
    #       arrayMap((x,y) -> (x,y), tags.key, tags.value)
    #  )) as all_tags)[1]
    return tupleElement(
        alias,
        arrayJoin(
            pair_alias,
            filter_key_values(
                zip_columns(
                    ColumnExpr(None, None, key_column(column_name)),
                    ColumnExpr(None, None, val_column(column_name)),
                ),
                filtered_tags,
            ),
        ),
        array_index,
    )


def _filtered_mapping_keys(
    alias: Optional[str], column_name: str, filtered_tags: Sequence[LiteralExpr]
) -> Expression:
    # arrayJoin(arrayFilter(
    #   tag -> tag IN (tags),
    #   tags.key
    # ))
    return arrayJoin(
        alias,
        filter_keys(ColumnExpr(None, None, key_column(column_name)), filtered_tags),
    )


def zip_columns(column1: ColumnExpr, column2: ColumnExpr) -> Expression:
    """
    Turns two array columns into an array of pairs
    """
    return FunctionCallExpr(
        None,
        "arrayMap",
        (
            Lambda(
                None,
                ("x", "y"),
                FunctionCallExpr(
                    None, "tuple", (Argument(None, "x"), Argument(None, "y"),),
                ),
            ),
            column1,
            column2,
        ),
    )


def filter_key_values(
    key_values: Expression, keys: Sequence[LiteralExpr]
) -> Expression:
    """
    Filter an array of key value pairs based on a sequence of keys
    (tag keys in this case).
    """
    return FunctionCallExpr(
        None,
        "arrayFilter",
        (
            Lambda(
                None,
                ("pair",),
                in_condition(
                    None,
                    # A pair here is a tuple with two elements (key
                    # and value) and the index of the first element in
                    # Clickhouse is 1 instead of 0.
                    tupleElement(None, Argument(None, "pair"), LiteralExpr(None, 1),),
                    keys,
                ),
            ),
            key_values,
        ),
    )


def filter_keys(column: Expression, keys: Sequence[LiteralExpr]) -> Expression:
    """
    Filter a Column array based on a sequence of keys.
    """
    return FunctionCallExpr(
        None,
        "arrayFilter",
        (
            Lambda(None, ("tag",), in_condition(None, Argument(None, "tag"), keys),),
            column,
        ),
    )
