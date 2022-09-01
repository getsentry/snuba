from typing import Set

from snuba.query.expressions import Expression, FunctionCall, Literal
from snuba.query.processors.physical.type_converters import (
    BaseTypeConverter,
    ColumnTypeError,
)


class FixedStringArrayColumnProcessor(BaseTypeConverter):
    def __init__(self, columns: Set[str], fixed_length: int):
        self.fixed_length = fixed_length
        super().__init__(columns, optimize_ordering=True)

    def _translate_literal(self, exp: Literal) -> Expression:
        try:
            assert isinstance(exp.value, str)
            return FunctionCall(
                exp.alias,
                "toFixedString",
                (Literal(None, value=exp.value), Literal(None, self.fixed_length)),
            )
        except (AssertionError, ValueError):
            raise ColumnTypeError("Not a valid UUID string", should_report=False)

    def _process_expressions(self, exp: Expression) -> Expression:
        # FixedString is converted to regular string just fine in query return
        # values.
        return exp
