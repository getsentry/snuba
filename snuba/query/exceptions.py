from typing import Optional

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
        cls,
        expression: Expression,
        message: str,
        should_report: bool = True,
    ) -> "InvalidExpressionException":
        return cls(message, should_report=should_report, expression=repr(expression))


class InvalidGranularityException(InvalidQueryException):
    pass


class QueryPlanException(SerializableException):
    """
    Common parent class used for exceptions outside the query flow.
    This includes exceptions from query plan builder.
    """

    def __init__(
        self,
        exception_type: Optional[str] = None,
        message: Optional[str] = None,
        should_report: bool = True,
    ) -> None:
        self.exception_type = exception_type
        super().__init__(message, should_report)

    @classmethod
    def from_args(cls, exception_type: str, message: str) -> "QueryPlanException":
        return cls(exception_type=exception_type, message=message)
