from snuba.query.expressions import Expression
from snuba.utils.snuba_exception import SnubaException


class InvalidQueryException(SnubaException):
    """
    Common parent class used for invalid queries during parsing
    and validation.
    This should not be used for system errors.

    """


class ValidationException(InvalidQueryException):
    pass


class InvalidExpressionException(ValidationException):
    # TODO (figure out what do do with this)
    @classmethod
    def from_args(
        cls, expression: Expression, message: str, should_report: bool,
    ) -> "InvalidExpressionException":
        return cls(message, should_report=should_report, expression=repr(expression))
