from snuba.query.expressions import Expression
from snuba.utils.snuba_exception import JsonSerializable, SnubaException


class InvalidQueryException(SnubaException):
    # TODO (figure out what do do with this)
    """
    Common parent class used for invalid queries during parsing
    and validation.
    This should not be used for system errors.

    Attributes:
        message: Message of the exception
        report: Should we report the exception to Sentry or not
    """

    @classmethod
    def from_args(
        cls, *, message: str, report: bool = True, **kwargs: JsonSerializable
    ) -> "InvalidQueryException":
        return cls(message=message, report=report, **kwargs)

    def __str__(self) -> str:
        return f"{self.message}"


class ValidationException(InvalidQueryException):
    pass


class InvalidExpressionException(ValidationException):
    # TODO (figure out what do do with this)
    def __init__(
        self, expression: Expression, message: str, report: bool = True
    ) -> None:
        self.expression = expression
        self.message = message
        super().__init__(message, report=report)

    def __str__(self) -> str:
        return f"Invalid Expression {self.expression}: {self.message}"
