from snuba.query.expressions import (
    Column,
    Expression,
    FunctionCall,
    Literal,
    Lambda,
    Argument,
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
        if isinstance(exp, Column) and exp.column_name in self.columns:
            return FunctionCall(
                exp.alias,
                "arrayMap",
                (
                    Lambda(
                        None,
                        ("x",),
                        FunctionCall(None, "toString", (Argument(None, "x"),)),
                    ),
                    Column(None, None, exp.column_name),
                ),
            )

        return exp
