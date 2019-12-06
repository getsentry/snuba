import re

from enum import Enum
from typing import Optional, Sequence, Tuple

from snuba.query.query import Query
from snuba.query.query_processor import QueryProcessor
from snuba.datasets.transactions_processor import escape_field
from snuba.query.types import Condition
from snuba.request.request_settings import RequestSettings


class Operand(Enum):
    EQ = "="
    NEQ = "!="


class CollapsedNestedFieldOptimizer(QueryProcessor):
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

    This means that a simple EQ/NEQ condition like tags[tag1] == 1 can be
    transformed into tags_map LIKE "%tag1:2%"
    """

    def __init__(self, nested_col: str, merged_col: str) -> None:
        self.__nested_col = nested_col
        self.__merged_col = merged_col

    def __is_optimizable(
        self, condition: Condition, column: str
    ) -> Optional[Tuple[str, str, Operand]]:
        """
        Recognize if the condition can be optimized.
        This includes these kind of conditions:
        - top level conditions. No nested OR
        - the condition has to be either in the form tag[t] = value
        - functions referencing the tags as parameters are not taken
          into account except for ifNull.
        - Both EQ and NEQ conditions are optimized.
        """
        if not isinstance(condition, (list, tuple)) or len(condition) != 3:
            return None
        if condition[1] not in ["=", "!="]:
            return None
        if not isinstance(condition[2], str):
            return None
        lhr = condition[0]
        if isinstance(lhr, list) and lhr[0] == "ifNull":
            lhr = lhr[1][0]
        if not isinstance(lhr, str):
            return None
        if lhr.startswith(f"{column}["):
            tag = re.match(r".+\[(.+)\]", lhr)
            if not tag:
                return None
            operand = Operand.EQ if condition[1] == "=" else Operand.NEQ
            return (tag[1], condition[2], operand)
        return None

    def process_query(self, query: Query, request_settings: RequestSettings,) -> None:
        conditions = query.get_conditions()
        if not conditions:
            return

        new_conditions = []
        positive_like_expression: Sequence[str] = []
        negative_like_expression: Sequence[str] = []

        for c in conditions:
            keyvalue = self.__is_optimizable(c, self.__nested_col)
            if not keyvalue:
                new_conditions.append(c)
            else:
                expression = f"{escape_field(keyvalue[0])}:{escape_field(keyvalue[1])}"
                if keyvalue[2] == Operand.EQ:
                    positive_like_expression.append(expression)
                else:
                    negative_like_expression.append(expression)

        if positive_like_expression:
            # Positive conditions "=" are all merged together in one LIKE expression
            like_formatted = f"%{'%'.join(positive_like_expression)}%"
            new_conditions.append([self.__merged_col, "LIKE", like_formatted])

        for expression in negative_like_expression:
            # Negative conditions "!=" cannot be merged together. We can still transform
            # them into NOT LIKE statements, but each condition has to be one
            # statement.
            not_like_formatted = f"%{expression}%"
            new_conditions.append([self.__merged_col, "NOT LIKE", not_like_formatted])

        query.set_conditions(new_conditions)
