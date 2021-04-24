from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.processors.type_converters import BaseTypeConverter, ColumnTypeError


class HexIntColumnProcessor(BaseTypeConverter):
    def _translate_literal(self, exp: Literal) -> Literal:
        try:
            assert isinstance(exp.value, str)
            return Literal(alias=exp.alias, value=int(exp.value, 16))
        except (AssertionError, ValueError):
            raise ColumnTypeError("Invalid hexint")

    def _process_expressions(self, exp: Expression) -> Expression:
        if isinstance(exp, Column) and exp.column_name in self.columns:
            return FunctionCall(
                exp.alias,
                "lower",
                (FunctionCall(None, "hex", (Column(None, None, exp.column_name),),),),
            )

        return exp
