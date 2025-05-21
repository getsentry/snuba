from typing import Set

from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, if_cond, literal
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
        """
        size is the number of characters in the hex string representation of the integer (e.g. 32 for 128 bit integers)
        """
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
            hex = f.hex(column(exp.column_name))
            return f.lower(
                f.leftPad(
                    hex,
                    if_cond(
                        f.greater(
                            f.length(hex),
                            literal(16),
                        ),
                        literal(32),
                        literal(16),
                    ),
                    literal("0"),
                ),
                alias=exp.alias,
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
