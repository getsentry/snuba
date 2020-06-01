import logging
from datetime import datetime
from enum import Enum
from typing import List, NamedTuple, Optional, Set

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.clickhouse.translators.snuba.mappers import KEY_COL_TAG_PARAM, tag_pattern
from snuba.datasets.events_format import escape_field
from snuba.query.expressions import Expression
from snuba.query.parser.strings import NESTED_COL_EXPR_RE
from snuba.query.types import Condition
from snuba.request.request_settings import RequestSettings
from snuba.util import is_condition, is_function, parse_datetime


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

    def __has_tags(self, expression: Optional[Expression]) -> bool:
        if not expression:
            return False
        for node in expression:
            match = tag_pattern.match(node)
            if match is not None:
                key_column_split = match.string(KEY_COL_TAG_PARAM).split(".")
                if key_column_split[0] == self.__nested_col:
                    return True
        return False

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        conditions = query.get_conditions()
        if not conditions:
            return

        # Enable the processor only if we have enough data in the flattened
        # columns. Which have been deployed at BEGINNING_OF_TIME. If the query
        # starts earlier than that we do not apply the optimization.
        if self.__beginning_of_time:
            apply_optimization = False
            for condition in conditions:
                if (
                    is_condition(condition)
                    and isinstance(condition[0], str)
                    and condition[0] in self.__timestamp_cols
                    and condition[1] in (">=", ">")
                    and isinstance(condition[2], str)
                ):
                    try:
                        start_ts = parse_datetime(condition[2])
                        if (start_ts - self.__beginning_of_time).total_seconds() > 0:
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

        new_conditions = []
        positive_like_expression: List[str] = []
        negative_like_expression: List[str] = []

        for c in conditions:
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
