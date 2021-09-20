from snuba.utils.snuba_exception import SnubaException
from snuba.query.expressions import Expression


class InvalidQueryException(Exception):
    """
    Common parent class used for invalid queries during parsing
    and validation.
    This should not be used for system errors.

    Attributes:
        message: Message of the exception
        report: Should we report the exception to Sentry or not
    """

    def __init__(self, message: str, *, report: bool = True):
        self.message = message
        self.report = report
        super().__init__(self.message, self.report)

    def __str__(self) -> str:
        return f"{self.message}"


class ValidationException(InvalidQueryException):
    pass


class InvalidExpressionException(ValidationException):
    def __init__(
        self, expression: Expression, message: str, report: bool = True
    ) -> None:
        self.expression = expression
        self.message = message
        super().__init__(message, report=report)

    def __str__(self) -> str:
        return f"Invalid Expression {self.expression}: {self.message}"
