from snuba.query.expressions import (
    Expression,
    FunctionCall,
    Literal,
)
from snuba.query.processors.type_converters import BaseTypeConverter, ColumnTypeError


class FixedStringArrayColumnProcessor(BaseTypeConverter):
    def _translate_literal(self, exp: Literal) -> Expression:
        try:
            assert isinstance(exp.value, str)
            return FunctionCall(
                exp.alias,
                "toFixedString",
                (Literal(None, value=exp.value), Literal(None, 32)),
            )
        except (AssertionError, ValueError):
            raise ColumnTypeError("Not a valid UUID string")

    def _process_expressions(self, exp: Expression) -> Expression:
        # FixedString is converted to regular string just fine in query return
        # values.
        return exp
