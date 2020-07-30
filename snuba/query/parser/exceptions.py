class InvalidQueryException(Exception):
    """
    Common parent class used for invalid queries during parsing
    and validation.
    This should not be used for system errors.
    """

    pass


class ParsingException(InvalidQueryException):
    pass


class ValidationException(InvalidQueryException):
    pass


class CyclicAliasException(ValidationException):
    pass


class AliasShadowingException(ValidationException):
    pass


class FunctionValidationException(ValidationException):
    pass
