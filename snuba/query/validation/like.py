from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.columns import String as StringType
from snuba.query.expressions import Expression
from snuba.query.matchers import Any, Column, FunctionCall, Literal, Or, Param, String
from snuba.query.parser.exceptions import ValidationException
from snuba.query.validation import ExpressionValidator

PATTERN = FunctionCall(
    None,
    Or([Param("func_name", String("like")), Param("func_name", String("notLike"))]),
    (
        Column(
            alias=None, table_name=None, column_name=Param("column_name", Any(str)),
        ),
        Literal(alias=None, value=None),
    ),
)


class LikeFunctionValidator(ExpressionValidator):
    """
    Validates function calls `like(column, 'literal')`.
    """

    def validate(self, exp: Expression, schema: ColumnSet) -> None:
        match = PATTERN.match(exp)
        if match is None:
            return

        column_name = match.string("column_name")
        column = schema.get(column_name)
        if column is None:
            # TODO: We cannot raise exceptions if the column is not present
            # on the schema just yet because the current logical schemas are
            # sadly not complete. Fix them and then raise an exception in this
            # case.
            return

        column_type = column.type.get_raw()
        if not isinstance(column_type, StringType):
            raise ValidationException(
                (
                    f"Illegal type {type(column_type)} of argument of "
                    f"function {match.string('func_name')}. "
                    f"Column {column_name}"
                )
            )
