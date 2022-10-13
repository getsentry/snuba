import uuid
from typing import Set

from snuba.query.expressions import (
    Argument,
    Column,
    Expression,
    FunctionCall,
    Lambda,
    Literal,
)
from snuba.query.processors.physical.type_converters import (
    BaseTypeConverter,
    ColumnTypeError,
)


class UUIDArrayColumnProcessor(BaseTypeConverter):
    def __init__(self, columns: Set[str]) -> None:
        super().__init__(columns)

    def _translate_literal(self, exp: Literal) -> Expression:
        try:
            assert isinstance(exp.value, str)
            new_val = str(uuid.UUID(exp.value))
            return FunctionCall(exp.alias, "toUUID", (Literal(None, value=new_val),))
        except (AssertionError, ValueError):
            raise ColumnTypeError("Not a valid UUID string", should_report=False)

    def _process_expressions(self, exp: Expression) -> Expression:
        if isinstance(exp, Column) and exp.column_name in self.columns:
            return FunctionCall(
                exp.alias,
                "arrayMap",
                (
                    Lambda(
                        None,
                        ("x",),
                        FunctionCall(
                            None,
                            "replaceAll",
                            (
                                FunctionCall(None, "toString", (Argument(None, "x"),)),
                                Literal(None, "-"),
                                Literal(None, ""),
                            ),
                        ),
                    ),
                    Column(None, None, exp.column_name),
                ),
            )

        return exp
