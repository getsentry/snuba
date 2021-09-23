from snuba.query.expressions import Expression
from snuba.utils.serializable_exception import SerializableException


class InvalidQueryException(SerializableException):
    """
    Common parent class used for invalid queries during parsing
    and validation.
    This should not be used for system errors.

    """


class ValidationException(InvalidQueryException):
    pass


class InvalidExpressionException(ValidationException):
    @classmethod
    def from_args(
        cls, expression: Expression, message: str, should_report: bool = True,
    ) -> "InvalidExpressionException":
        return cls(message, should_report=should_report, expression=repr(expression))
