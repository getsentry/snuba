from snuba.query.expressions import Expression
from snuba.utils.snuba_exception import SnubaException


class InvalidQueryException(SnubaException):
    """
    Common parent class used for invalid queries during parsing
    and validation.
    This should not be used for system errors.

    Attributes:
        message: Message of the exception
        report: Should we report the exception to Sentry or not
    """

    def __str__(self) -> str:
        return f"{self.message}"


class ValidationException(InvalidQueryException):
    pass


class InvalidExpressionException(ValidationException):
    # TODO (figure out what do do with this)
    @classmethod
    def from_args(
        cls, message: str, expression: Expression, should_report: bool,
    ) -> "InvalidExpressionException":
        return cls(message, should_report=should_report, expression=repr(Expression))
