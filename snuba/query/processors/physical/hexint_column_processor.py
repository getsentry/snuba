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


class HexIntColumnProcessor(BaseTypeConverter):
    def __init__(self, columns: Set[str], size: int = 16) -> None:
        super().__init__(columns, optimize_ordering=True)
        self._size = size

    def _translate_literal(self, exp: Literal) -> Literal:
        try:
            assert isinstance(exp.value, str)
            # 128 bit integers in clickhouse need to be referenced as strings
            if self._size == 32:
                return Literal(alias=exp.alias, value=str(int(exp.value, 16)))
            return Literal(alias=exp.alias, value=int(exp.value, 16))
        except (AssertionError, ValueError):
            raise ColumnTypeError("Invalid hexint", should_report=False)

    def _process_expressions(self, exp: Expression) -> Expression:
        if isinstance(exp, Column) and exp.column_name in self.columns:
            return FunctionCall(
                exp.alias,
                "leftPad",
                (
                    FunctionCall(
                        None,
                        "lower",
                        (
                            FunctionCall(
                                None,
                                "hex",
                                (Column(None, None, exp.column_name),),
                            ),
                        ),
                    ),
                    Literal(None, self._size),
                    Literal(None, "0"),
                ),
            )
        return exp


class HexIntArrayColumnProcessor(BaseTypeConverter):
    def _translate_literal(self, exp: Literal) -> Literal:
        try:
            assert isinstance(exp.value, str)
            return Literal(alias=exp.alias, value=int(exp.value, 16))
        except (AssertionError, ValueError):
            raise ColumnTypeError("Invalid hexint", report=False)

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
                            "lower",
                            (FunctionCall(None, "hex", (Argument(None, "x"),)),),
                        ),
                    ),
                    Column(None, None, exp.column_name),
                ),
            )

        return exp
