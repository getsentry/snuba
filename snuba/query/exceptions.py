from snuba.query.expressions import Expression


class InvalidQueryException(Exception):
    """
    Common parent class used for invalid queries during parsing
    and validation.
    This should not be used for system errors.
    """

    pass


class ValidationException(InvalidQueryException):
    pass


class InvalidExpressionException(ValidationException):
    def __init__(self, expression: Expression, message: str) -> None:
        self.expression = expression
        self.message = message
        super().__init__(message)

    def __str__(self) -> str:
        return f"Invalid Expression {self.expression}: {self.message}"
