import uuid


from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.processors.type_converters import BaseTypeConverter, ColumnTypeError


class UUIDColumnProcessor(BaseTypeConverter):
    def _translate_literal(self, exp: Literal) -> Literal:
        try:
            assert isinstance(exp, Literal)
            assert isinstance(exp.value, str)
            new_val = str(uuid.UUID(exp.value))
            return Literal(alias=exp.alias, value=new_val)
        except (AssertionError, ValueError):
            raise ColumnTypeError("Not a valid UUID string")

    def _process_expressions(self, exp: Expression) -> Expression:
        if isinstance(exp, Column) and exp.column_name in self.columns:
            return FunctionCall(
                exp.alias,
                "replaceAll",
                (
                    FunctionCall(
                        None, "toString", (Column(None, None, exp.column_name),),
                    ),
                    Literal(None, "-"),
                    Literal(None, ""),
                ),
            )

        return exp
