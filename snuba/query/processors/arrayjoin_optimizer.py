from typing import Optional, Set, cast

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.conditions import (
    OPERATOR_TO_FUNCTION,
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

array_join_pattern = FunctionCall(
    None, String("arrayJoin"), (Column(None, None, String("tags.key")),)
)


def extract_top_level_tag_conditions(condition: Expression) -> Set[str]:
    """
    Finds the top level conditions that include tags_key in an expression.
    For top level condition. In Snuba conditions are expressed in layers, the top level
    ones are in AND, the nested ones are in OR and so on.
    We can only apply the arrayFilter optimization to tag keys conditions that are not in
    OR with other columns. To simplify the problem, we only consider those conditions that
    are included in the first level of the query:
    [['tagskey' '=' 'a'],['col' '=' 'b'],['col2' '=' 'c']]  works
    [[['tagskey' '=' 'a'], ['col2' '=' 'b']], ['tagskey' '=' 'c']] does not
    """
    tags_found = set()
    conditions = get_first_level_conditions(condition)
    for c in conditions:
        match = FunctionCall(
            None,
            String(ConditionFunctions.EQ),
            (array_join_pattern, Literal(None, Param("key", Any(str)))),
        ).match(c)
        if match is not None:
            tags_found.add(match.string("key"))

        match = is_in_condition_pattern(array_join_pattern).match(c)
        if match is not None:
            function = cast(FunctionCallExpr, match.expression("tuple"))
            tags_found |= {
                l.value for l in function.parameters if isinstance(l, LiteralExpr)
            }

    return tags_found


def get_filter_tags(query: Query) -> Set[str]:
    """
        Identifies the tag names we can apply the arrayFilter optimization on.
        Which means: if the tags_key column is in the select clause and there are
        one or more top level conditions on the tags_key column.
        """
    select_clause = query.get_selected_columns_from_ast() or []

    tags_key_found = any(
        array_join_pattern.match(f) is not None
        for expression in select_clause
        for f in expression
    )

    if not tags_key_found:
        return set()

    def extract_tags_from_condition(cond: Optional[Expression]) -> Optional[Set[str]]:
        if not cond:
            return set()
        if any(is_binary_condition(cond, BooleanFunctions.OR) for cond in cond):
            return None
        return extract_top_level_tag_conditions(cond)

    cond_tags_key = extract_tags_from_condition(query.get_condition_from_ast())
    if cond_tags_key is None:
        # This means we found an OR. Cowardly we give up even though there could
        # be cases where this condition is still optimizable.
        return set()
    having_tags_key = extract_tags_from_condition(query.get_having_from_ast())
    if having_tags_key is None:
        # Same as above
        return set()

    return cond_tags_key.union(having_tags_key)


class ArrayjoinOptimizer(QueryProcessor):
    """
    Apply two optimizations to reduce the performance impact of tags_key
    and tags_value expressions that are sent to clickhouse as
    arrayJoin(tags.key)

    - it performs the arrayJoin only once in case both tags_key and
        tags_value are present by arrayJoining on on the key, value
        tuples instead of arrayJoining twice, once on keys and the second
        on values.
    - prefilter the keys with arrayFilter inside the arrayJoin if there
        are conditions in the query that specify which tags we want
        instead of doing the entire arrayJoin and filter the results.
    """

    def __build_positional_access(
        self, alias: Optional[str], content: Expression, position: int
    ) -> Expression:
        return arrayElement(
            alias,
            FunctionCallExpr("all_tags", "arrayJoin", (content,),),
            LiteralExpr(None, position),
        )

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        matcher = FunctionCall(
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

        referenced_tags = set()
        for e in query.get_all_expressions():
            match = matcher.match(e)
            if match is not None:
                referenced_tags.add(match.string("col"))

        filtered_tags = [
            LiteralExpr(None, tag_name) for tag_name in get_filter_tags(query)
        ]

        def replace_expression(expr: Expression) -> Expression:
            match = matcher.match(expr)
            if match is None:
                return expr

            if len(referenced_tags) == 2:
                array_pos = 1 if match.string("col") == "tags.key" else 2
                two_col_map = FunctionCallExpr(
                    None,
                    "arrayMap",
                    (
                        Lambda(
                            None,
                            ("x", "y"),
                            FunctionCallExpr(
                                None,
                                "tuple",
                                (Argument(None, "x"), Argument(None, "y"),),
                            ),
                        ),
                        ColumnExpr(None, None, "tags.key"),
                        ColumnExpr(None, None, "tags.value"),
                    ),
                )
                if not filtered_tags:
                    # arrayJoin(arrayMap((x,y) -> [x,y], tags.key, tags.value)
                    return self.__build_positional_access(
                        expr.alias,
                        FunctionCallExpr("all_tags", "arrayJoin", (two_col_map,)),
                        array_pos,
                    )
                else:
                    # arrayJoin(
                    #   arrayFilter(
                    #       pair -> arrayElement(pair, 1) IN (tags),
                    #       arrayMap((x,y) -> [x,y], tags.key, tags.value)
                    #   )
                    # )
                    return self.__build_positional_access(
                        expr.alias,
                        FunctionCallExpr(
                            None,
                            "arrayFilter",
                            (
                                Lambda(
                                    None,
                                    ("pair",),
                                    in_condition(
                                        None,
                                        arrayElement(
                                            None,
                                            Argument(None, "pair"),
                                            LiteralExpr(None, 1),
                                        ),
                                        filtered_tags,
                                    ),
                                ),
                                two_col_map,
                            ),
                        ),
                        array_pos,
                    )

            elif filtered_tags:
                # arrayJoin(
                #   arrayFilter(
                #       tag -> tag IN (tags),
                #       tags.key
                #   )
                # )
                return FunctionCallExpr(
                    expr.alias,
                    "arrayJoin",
                    (
                        FunctionCallExpr(
                            None,
                            "arrayFilter",
                            (
                                Lambda(
                                    None,
                                    ("tag",),
                                    in_condition(
                                        None, Argument(None, "tag"), filtered_tags
                                    ),
                                ),
                                ColumnExpr(None, None, "tags.key"),
                            ),
                        ),
                    ),
                )
            else:
                return expr

        query.transform_expressions(replace_expression)
