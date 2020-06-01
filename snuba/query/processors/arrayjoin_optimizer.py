from typing import Optional, Sequence, Set, cast

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    get_first_level_conditions,
    in_condition,
    is_binary_condition,
    is_in_condition_pattern,
)
from snuba.query.dsl import arrayElement
from snuba.query.expressions import Argument
from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import Expression
from snuba.query.expressions import FunctionCall as FunctionCallExpr
from snuba.query.expressions import Lambda
from snuba.query.expressions import Literal as LiteralExpr
from snuba.query.matchers import Any, Column, FunctionCall, Literal, Or, Param, String
from snuba.request.request_settings import RequestSettings

tags_key_pattern = FunctionCall(
    None, String("arrayJoin"), (Column(None, None, String("tags.key")),)
)


def _get_tag_keys_in_condition(condition: Expression) -> Optional[Set[str]]:
    """
    Finds the top level conditions that include filter based on tags_key.
    This is meant to be used to find the tag keys the query is filtering on.
    We can only apply the arrayFilter optimization to tag keys conditions
    that are not in OR with other columns. To simplify the problem, we only
    consider those conditions that are included in the first level of the query:
    [['tagskey' '=' 'a'],['col' '=' 'b'],['col2' '=' 'c']]  works
    [[['tagskey' '=' 'a'], ['col2' '=' 'b']], ['tagskey' '=' 'c']] does not

    If we encounter an OR condition we return None, which means we cannot
    safely apply the optimization. Empty set means we did not find any
    suitable tag key for optimization in this condition but that does
    not disqualify the whole query in the way the OR condition does.
    """
    tag_keys_found = set()

    conditions = get_first_level_conditions(condition)
    for c in conditions:
        if is_binary_condition(c, BooleanFunctions.OR):
            return None

        match = FunctionCall(
            None,
            String(ConditionFunctions.EQ),
            (tags_key_pattern, Literal(None, Param("key", Any(str)))),
        ).match(c)
        if match is not None:
            tag_keys_found.add(match.string("key"))

        match = is_in_condition_pattern(tags_key_pattern).match(c)
        if match is not None:
            function = cast(FunctionCallExpr, match.expression("tuple"))
            tag_keys_found |= {
                l.value
                for l in function.parameters
                if isinstance(l, LiteralExpr) and isinstance(l.value, str)
            }

    return tag_keys_found


def get_filtered_tag_keys(query: Query) -> Set[str]:
    """
    Identifies the tag names we can apply the arrayFilter optimization on.
    Which means: if the tags_key column is in the select clause, there
    are one or more top level AND condition on the tags_key column and
    there is no OR condition in the query.
    """
    tags_key_found = any(
        tags_key_pattern.match(f) is not None
        for expression in query.get_selected_columns_from_ast() or []
        for f in expression
    )

    if not tags_key_found:
        return set()

    ast_condition = query.get_condition_from_ast()
    cond_tags_key = (
        _get_tag_keys_in_condition(ast_condition)
        if ast_condition is not None
        else set()
    )
    if cond_tags_key is None:
        # This means we found an OR. Cowardly we give up even though there could
        # be cases where this condition is still optimizable.
        return set()

    ast_having = query.get_having_from_ast()
    having_tags_key = (
        _get_tag_keys_in_condition(ast_having) if ast_having is not None else set()
    )
    if having_tags_key is None:
        # Same as above
        return set()

    return cond_tags_key | having_tags_key


class ArrayjoinOptimizer(QueryProcessor):
    """
    Applies two optimizations to reduce the performance impact of
    tags_key and tags_value expressions that are sent to clickhouse
    as arrayJoin(tags.key)

    - it performs the arrayJoin only once in case both tags_key and
        tags_value are present in the query by arrayJoining on the
        key, value pairs instead of arrayJoining twice, once on keys
        and the second on values.
    - prefilter the keys with arrayFilter inside the arrayJoin if there
        are conditions in the query that specify which tags we want
        instead of doing the entire arrayJoin and filter the results.
    """

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        arrayjoin_pattern = FunctionCall(
            None,
            String("arrayJoin"),
            (
                Column(
                    None,
                    None,
                    Param("col", Or([String("tags.key"), String("tags.value")])),
                ),
            ),
        )

        arrayjoins_in_query = set()
        for e in query.get_all_expressions():
            match = arrayjoin_pattern.match(e)
            if match is not None:
                arrayjoins_in_query.add(match.string("col"))

        filtered_tags = [
            LiteralExpr(None, tag_name) for tag_name in get_filtered_tag_keys(query)
        ]

        def replace_expression(expr: Expression) -> Expression:
            """
            Applies the appropriate optimization on a single arrayJoin expression.
            """
            match = arrayjoin_pattern.match(expr)
            if match is None:
                return expr

            if arrayjoins_in_query == {"tags.key", "tags.value"}:
                # Both tags_key and tags_value expressions present int the query.
                # do the arrayJoin on key-value pairs instead of independent
                # arrayjoin for keys and values.
                array_index = (
                    LiteralExpr(None, 1)
                    if match.string("col") == "tags.key"
                    else LiteralExpr(None, 2)
                )

                if not filtered_tags:
                    return _unfiltered_tag_pairs(expr.alias, array_index)
                else:
                    return _filtered_tag_pairs(expr.alias, filtered_tags, array_index)

            elif filtered_tags:
                # Only one between tags_key and tags_value is present, and
                # it is tags_key since we found filtered tag keys.
                return _filtered_tag_keys(expr.alias, filtered_tags)
            else:
                # No viable optimization
                return expr

        query.transform_expressions(replace_expression)


def _unfiltered_tag_pairs(alias: Optional[str], array_index: LiteralExpr) -> Expression:
    # (arrayJoin(
    #   arrayMap((x,y) -> [x,y], tags.key, tags.value)
    #  as all_tags)[1]
    return arrayElement(
        alias,
        array_join(
            "all_tags",
            zip_columns(
                ColumnExpr(None, None, "tags.key"),
                ColumnExpr(None, None, "tags.value"),
            ),
        ),
        array_index,
    )


def _filtered_tag_pairs(
    alias: Optional[str],
    filtered_tags: Sequence[LiteralExpr],
    array_index: LiteralExpr,
) -> Expression:
    # (arrayJoin(arrayFilter(
    #       pair -> arrayElement(pair, 1) IN (tags),
    #       arrayMap((x,y) -> [x,y], tags.key, tags.value)
    #  )) as all_tags)[1]
    return arrayElement(
        alias,
        array_join(
            "all_tags",
            filter_key_values(
                zip_columns(
                    ColumnExpr(None, None, "tags.key"),
                    ColumnExpr(None, None, "tags.value"),
                ),
                filtered_tags,
            ),
        ),
        array_index,
    )


def _filtered_tag_keys(
    alias: Optional[str], filtered_tags: Sequence[LiteralExpr]
) -> Expression:
    # arrayJoin(arrayFilter(
    #   tag -> tag IN (tags),
    #   tags.key
    # ))
    return array_join(
        alias, filter_keys(ColumnExpr(None, None, "tags.key"), filtered_tags),
    )


def array_join(alias: Optional[str], content: Expression) -> Expression:
    return FunctionCallExpr(alias, "arrayJoin", (content,))


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
                    None, "array", (Argument(None, "x"), Argument(None, "y"),),
                ),
            ),
            column1,
            column2,
        ),
    )


def filter_key_values(
    key_values: Expression, tag_keys: Sequence[LiteralExpr]
) -> Expression:
    """
    Filter a an array of key value pairs based on a sequence of keys
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
                    # A pair here is an array with two elements (key
                    # and value) and the index of the first element in
                    # Clickhouse is 1 instead of 0.
                    arrayElement(None, Argument(None, "pair"), LiteralExpr(None, 1),),
                    tag_keys,
                ),
            ),
            key_values,
        ),
    )


def filter_keys(keys: Expression, tag_keys: Sequence[LiteralExpr]) -> Expression:
    """
    Filter a an array of tag keys based on a sequence of tag keys.
    """
    return FunctionCallExpr(
        None,
        "arrayFilter",
        (
            Lambda(
                None, ("tag",), in_condition(None, Argument(None, "tag"), tag_keys),
            ),
            keys,
        ),
    )
