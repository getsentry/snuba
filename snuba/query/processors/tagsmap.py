import logging

from datetime import datetime
from typing import cast, TypeVar, Callable, Sequence

from enum import Enum
from typing import Optional, List, NamedTuple, Set

from snuba.clickhouse.query import Query
from snuba.clickhouse.processors import QueryProcessor
from snuba.datasets.events_format import escape_field
from snuba.query.expressions import (
    Expression,
    SubscriptableReference,
    FunctionCall,
    Literal,
    Column,
)
from snuba.query.parser.strings import NESTED_COL_EXPR_RE
from snuba.query.types import Condition
from snuba.request.request_settings import RequestSettings
from snuba.util import is_condition, is_function, parse_datetime
from snuba.query.conditions import get_first_level_conditions, combine_and_conditions
from snuba.query.matchers import Column as ColumnPattern
from snuba.query.matchers import FunctionCall as FunctionCallPattern
from snuba.query.matchers import Literal as LiteralPattern
from snuba.query.matchers import Param, Or, Any, String
from snuba.query.conditions import ConditionFunctions


class Operand(Enum):
    EQ = "="
    NEQ = "!="


class OptimizableCondition(NamedTuple):
    nested_col_key: str
    operand: Operand
    value: str


logger = logging.getLogger(__name__)


class NestedFieldConditionOptimizer(QueryProcessor):
    """
    This processor scans the the conditions in the query and converts
    top level eq/neq conditions on specific nested fields, into LIKE/NOT LIKE
    statements on a column that merges all the content of a nested one.

    Example:
    tags: {
        "tag1": "1",
        "tag2": "2",
    }

    The merged columns looks like:
    tags_map: "tag1:1|tag2:2"

    This means that a simple EQ/NEQ condition like `tags[tag1] == 1` can be
    transformed into `tags_map LIKE "%tag1:2%"`
    """

    def __init__(
        self,
        nested_col: str,
        flattened_col: str,
        timestamp_cols: Set[str],
        beginning_of_time: Optional[datetime] = None,
    ) -> None:
        self.__nested_col = nested_col
        self.__flattened_col = flattened_col
        self.__timestamp_cols = timestamp_cols
        # This is the cutoff time when we started filling in the relevant column.
        # If a query goes back further than this date we cannot apply the optimization
        self.__beginning_of_time = beginning_of_time

    def __is_optimizable(
        self, condition: Condition, column: str
    ) -> Optional[OptimizableCondition]:
        """
        Recognize if the condition can be optimized.
        This includes these kind of conditions:
        - top level conditions. No nested OR
        - the condition has to be either in the form tag[t] = value
        - functions referencing the tags as parameters are not taken
          into account except for ifNull.
        - Both EQ and NEQ conditions are optimized.
        """
        if not is_condition(condition):
            return None
        if condition[1] not in [Operand.EQ.value, Operand.NEQ.value]:
            return None
        if not isinstance(condition[2], str):
            # We can only support literals for now.
            return None
        lhs = condition[0]

        # This unpacks the ifNull function. This is just an optimization to make this class more
        # useful since the product wraps tags access into ifNull very often and it is a trivial
        # function to unpack. We could exptend it to more functions later.
        function_expr = is_function(lhs, 0)
        if function_expr and function_expr[0] == "ifNull" and len(function_expr[1]) > 0:
            lhs = function_expr[1][0]
        if not isinstance(lhs, str):
            return None

        # Now we have a condition in the form of: ["tags[something]", "=", "a string"]
        tag = NESTED_COL_EXPR_RE.match(lhs)
        if tag and tag[1] == self.__nested_col:
            # tag[0] is the full expression that matches the re.
            nested_col_key = tag[2]
            return OptimizableCondition(
                nested_col_key=nested_col_key,
                operand=Operand.EQ if condition[1] == "=" else Operand.NEQ,
                value=condition[2],
            )
        return None

    def __is_ast_optimizable(
        self, condition: Expression, column: str
    ) -> Optional[OptimizableCondition]:
        match = FunctionCallPattern(
            None,
            Param(
                "operator",
                Or([String(ConditionFunctions.EQ), String(ConditionFunctions.NEQ)]),
            ),
            (
                Or(
                    [
                        FunctionCallPattern(
                            None,
                            String("ifNull"),
                            (Param("tag", Any(SubscriptableReference)),),
                        ),
                        (Param("tag", Any(SubscriptableReference))),
                    ]
                ),
                LiteralPattern(None, Param("tag_key", Any(str))),
            ),
        ).match(condition)

        if match is None:
            return None

        if (
            cast(SubscriptableReference, match.expression("tag")).column.column_name
            == self.__nested_col
        ):
            nested_col_key = cast(
                SubscriptableReference, match.expression("tag")
            ).key.value
            return OptimizableCondition(
                nested_col_key=str(nested_col_key),
                operand=Operand.EQ
                if match.string("operator") == ConditionFunctions.EQ
                else Operand.NEQ,
                value=match.string("tag_key"),
            )
        return None

    def __has_tags(self, expression: Optional[Expression]) -> bool:
        if expression is None:
            return False
        for node in expression:
            if isinstance(node, SubscriptableReference):
                # Unfortunately, being this a storage processor, as soon as we wrap query
                # translation, here we will only have the resolved tag, so this condition
                # will be more complex.
                if node.column.column_name == self.__nested_col:
                    return True
        return False

    CondType = TypeVar("CondType")

    def __apply_conditions(
        self,
        conditions: Sequence[CondType],
        optimizable_checker: Callable[[CondType], Optional[OptimizableCondition]],
        condition_builder: Callable[[str, str], CondType],
        conditions_writer: Callable[[Sequence[CondType]], None],
    ) -> None:
        new_conditions = []
        positive_like_expression: List[str] = []
        negative_like_expression: List[str] = []

        for c in conditions:
            keyvalue = optimizable_checker(c)
            if not keyvalue:
                new_conditions.append(c)
            else:
                expression = f"{escape_field(keyvalue.nested_col_key)}={escape_field(keyvalue.value)}"
                if keyvalue.operand == Operand.EQ:
                    positive_like_expression.append(expression)
                else:
                    negative_like_expression.append(expression)

        if positive_like_expression:
            # Positive conditions "=" are all merged together in one LIKE expression
            positive_like_expression = sorted(positive_like_expression)
            like_formatted = f"%|{'|%|'.join(positive_like_expression)}|%"
            new_conditions.append(condition_builder("LIKE", like_formatted))

        for expression in negative_like_expression:
            # Negative conditions "!=" cannot be merged together. We can still transform
            # them into NOT LIKE statements, but each condition has to be one
            # statement.
            not_like_formatted = f"%|{expression}|%"
            new_conditions.append(condition_builder("NOT LIKE", not_like_formatted))

        conditions_writer(new_conditions)

    def _apply_legacy_flattened(self, query: Query) -> None:
        new_conditions = []
        positive_like_expression: List[str] = []
        negative_like_expression: List[str] = []

        for c in query.get_conditions() or []:
            keyvalue = self.__is_optimizable(c, self.__nested_col)
            if not keyvalue:
                new_conditions.append(c)
            else:
                expression = f"{escape_field(keyvalue.nested_col_key)}={escape_field(keyvalue.value)}"
                if keyvalue.operand == Operand.EQ:
                    positive_like_expression.append(expression)
                else:
                    negative_like_expression.append(expression)

        if positive_like_expression:
            # Positive conditions "=" are all merged together in one LIKE expression
            positive_like_expression = sorted(positive_like_expression)
            like_formatted = f"%|{'|%|'.join(positive_like_expression)}|%"
            new_conditions.append([self.__flattened_col, "LIKE", like_formatted])

        for expression in negative_like_expression:
            # Negative conditions "!=" cannot be merged together. We can still transform
            # them into NOT LIKE statements, but each condition has to be one
            # statement.
            not_like_formatted = f"%|{expression}|%"
            new_conditions.append(
                [self.__flattened_col, "NOT LIKE", not_like_formatted]
            )

        query.set_conditions(new_conditions)

    def _apply_ast_flattened(self, query: Query) -> None:
        ast_condition = query.get_condition_from_ast()
        if ast_condition is None:
            return

        new_conditions = []
        positive_like_expression: List[str] = []
        negative_like_expression: List[str] = []

        conditions = get_first_level_conditions(ast_condition)
        for c in conditions:
            keyvalue = self.__is_ast_optimizable(c, self.__nested_col)
            if not keyvalue:
                new_conditions.append(c)
            else:
                expression = f"{escape_field(keyvalue.nested_col_key)}={escape_field(keyvalue.value)}"
                if keyvalue.operand == Operand.EQ:
                    positive_like_expression.append(expression)
                else:
                    negative_like_expression.append(expression)

        if positive_like_expression:
            # Positive conditions "=" are all merged together in one LIKE expression
            positive_like_expression = sorted(positive_like_expression)
            like_formatted = f"%|{'|%|'.join(positive_like_expression)}|%"
            new_conditions.append(
                FunctionCall(
                    None,
                    ConditionFunctions.LIKE,
                    (
                        Column(None, None, self.__flattened_col),
                        Literal(None, like_formatted),
                    ),
                )
            )

        for expression in negative_like_expression:
            # Negative conditions "!=" cannot be merged together. We can still transform
            # them into NOT LIKE statements, but each condition has to be one
            # statement.
            not_like_formatted = f"%|{expression}|%"
            new_conditions.append(
                FunctionCall(
                    None,
                    ConditionFunctions.NOT_LIKE,
                    (
                        Column(None, None, self.__flattened_col),
                        Literal(None, not_like_formatted),
                    ),
                )
            )

        query.set_ast_condition(combine_and_conditions(new_conditions))

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        ast_condition = query.get_condition_from_ast()
        if ast_condition is None:
            return

        conditions = get_first_level_conditions(ast_condition)

        # Enable the processor only if we have enough data in the flattened
        # columns. Which have been deployed at BEGINNING_OF_TIME. If the query
        # starts earlier than that we do not apply the optimization.
        if self.__beginning_of_time:
            apply_optimization = False
            for condition in conditions:
                match = FunctionCallPattern(
                    None,
                    Or([String(ConditionFunctions.GTE), String(ConditionFunctions.GT)]),
                    (
                        ColumnPattern(
                            column_name=Or([String(ts) for ts in self.__timestamp_cols])
                        ),
                        LiteralPattern(None, Param("timestamp", Any(datetime))),
                    ),
                ).match(condition)
                if match is not None:
                    try:
                        if (
                            cast(datetime, match.scalar("timestamp"))
                            - self.__beginning_of_time
                        ).total_seconds() > 0:
                            apply_optimization = True
                    except Exception:
                        # We should not get here, it means the from timestamp is malformed
                        # Returning here is just for safety
                        logger.error(
                            "Cannot parse start date for NestedFieldOptimizer: %r",
                            condition,
                        )
                        return
            if not apply_optimization:
                return

        # Do not use flattened tags if tags are being unpacked anyway. In that case
        # using flattened tags only implies loading an additional column thus making
        # the query heavier and slower
        if self.__has_tags(query.get_arrayjoin_from_ast()):
            return
        if query.get_groupby_from_ast():
            for expression in query.get_groupby_from_ast():
                if self.__has_tags(expression):
                    return
        if self.__has_tags(query.get_having_from_ast()):
            return

        if query.get_orderby_from_ast():
            for orderby in query.get_orderby_from_ast():
                if self.__has_tags(orderby.expression):
                    return

        self._apply_legacy_flattened(query)
        self._apply_ast_flattened(query)
