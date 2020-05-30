from typing import cast, Set, Optional

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.expressions import FunctionCall as FunctionCallExpr
from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import Lambda, Argument, LiteralExpr, Expression
from snuba.query.dsl import arrayElement
from snuba.query.matchers import FunctionCall, Column, String, Or, Param, Literal, Any
from snuba.request.request_settings import RequestSettings
from snuba.query.conditions import (
    OPERATOR_TO_FUNCTION,
    ConditionFunctions,
    combine_and_conditions,
    get_first_level_conditions,
    is_binary_condition,
    in_condition,
    BooleanFunctions,
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
        tag_eq_matcher = FunctionCall(
            None,
            String(ConditionFunctions.EQ),
            (
                FunctionCall(
                    None, String("arrayJoin"), (Column(None, None, String("tags.key")),)
                ),
                Literal(None, Param("key", Any(str))),
            ),
        )
        match = tag_eq_matcher.match(c)
        if match is not None:
            tags_found.add(match.string("key"))

        tag_in_matcher = FunctionCall(
            None,
            String(ConditionFunctions.IN),
            (
                FunctionCall(
                    None, String("arrayJoin"), (Column(None, None, String("tags.key")),)
                ),
                Param("literals", Any(FunctionCall)),
            ),
        )

        match = tag_in_matcher.match(c)
        if match is not None:
            function = cast(FunctionCallExpr, match.expression("literals"))
            if function.function_name == "tuple":
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
        FunctionCall(
            None, String("arrayJoin"), (Column(None, None, String("tags.key")),)
        ).match(f)
        is not None
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

        filtered_tags = get_filter_tags(query)

        referenced_tags = set()
        for e in query.get_all_expressions():
            match = matcher.match(e)
            if match is not None:
                referenced_tags.add(match.string("col"))

        replaced = None
        if len(referenced_tags) < 2:
            if not filtered_tags:
                return
            else:
                replaced = FunctionCallExpr(
                    "all_tags",
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
                                        None,
                                        Argument(None, "tag"),
                                        [
                                            LiteralExpr(None, tag_name)
                                            for tag_name in filtered_tags
                                        ],
                                    ),
                                ),
                                ColumnExpr(None, None, "tags.key"),
                            ),
                        ),
                    ),
                )
        else:
            mapping = FunctionCallExpr(
                None,
                "arrayMap",
                (
                    Lambda(
                        None,
                        ("x", "y"),
                        FunctionCallExpr(
                            None, "tuple", (Argument(None, "x"), Argument(None, "y")),
                        ),
                    ),
                    ColumnExpr(None, None, "tags.key"),
                    ColumnExpr(None, None, "tags.value"),
                ),
            )
            replaced = FunctionCallExpr(
                "all_tags",
                "arrayJoin",
                (
                    FunctionCallExpr(
                        None,
                        "arrayFilter",
                        (
                            Lambda(
                                None,
                                ("pair",),
                                in_condition(
                                    None,
                                    FunctionCallExpr(
                                        None,
                                        "arrayElement",
                                        (Argument(None, "pair"), LiteralExpr(None, 1)),
                                    ),
                                    [
                                        LiteralExpr(None, tag_name)
                                        for tag_name in filtered_tags
                                    ],
                                ),
                            ),
                            mapping,
                        ),
                    ),
                ),
            )

        if replaced is None:
            return

        def replace_expression(expr: Expression) -> Expression:
            match = matcher.match(expr)
            if match is None:
                return expr

            return arrayElement(
                expr.alias,
                replaced,
                LiteralExpr(None, 1)
                if match.string("col") == "tags.key"
                else LiteralExpr(None, 2),
            )

        query.transform_expressions(replace_expression)
