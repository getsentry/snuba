from typing import Any, Optional, Sequence

from snuba.datasets.entity import Entity
from snuba.datasets.entities.factory import get_entity_name
from snuba.query import Query
from snuba.query.conditions import (
    ConditionFunctions,
    get_first_level_and_conditions,
)
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Expression
from snuba.query.matchers import Any as AnyMatch
from snuba.query.matchers import AnyOptionalString
from snuba.query.matchers import Column as ColumnMatch
from snuba.query.matchers import FunctionCall as FunctionCallMatch
from snuba.query.matchers import Literal as LiteralMatch
from snuba.query.matchers import Or
from snuba.query.matchers import String as StringMatch
from snuba.query.parser.validation import QueryValidator


def build_match(
    col: str, ops: Sequence[str], param_type: Any, alias: Optional[str] = None
) -> Or[Expression]:
    # The IN condition has to be checked separately since each parameter
    # has to be checked individually.
    alias_match = AnyOptionalString() if alias is None else StringMatch(alias)
    column_match = ColumnMatch(alias_match, StringMatch(col))
    return Or(
        [
            FunctionCallMatch(
                Or([StringMatch(op) for op in ops]),
                (column_match, LiteralMatch(AnyMatch(param_type))),
            ),
            FunctionCallMatch(
                StringMatch(ConditionFunctions.IN),
                (
                    column_match,
                    FunctionCallMatch(
                        Or([StringMatch("array"), StringMatch("tuple")]),
                        all_parameters=LiteralMatch(AnyMatch(param_type)),
                    ),
                ),
            ),
        ]
    )


class EntityRequiredColumnValidator(QueryValidator):
    def validate(
        self, entity: Entity, query: Query, alias: Optional[str] = None
    ) -> None:
        if not entity.required_filter_columns and not entity.required_time_column:
            return None

        condition = query.get_condition()
        top_level = get_first_level_and_conditions(condition) if condition else []

        missing = set()
        if entity.required_filter_columns:
            for col in entity.required_filter_columns:
                match = build_match(col, [ConditionFunctions.EQ], int, alias)
                found = any(match.match(cond) for cond in top_level)
                if not found:
                    missing.add(col)

        if missing:
            raise InvalidQueryException(
                f"missing required conditions on {', '.join(missing)} for entity {get_entity_name(entity)}"
            )
