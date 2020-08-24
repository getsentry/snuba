from snuba.query.exceptions import InvalidQueryException, ValidationException


class ParsingException(InvalidQueryException):
    pass


class CyclicAliasException(ValidationException):
    pass


class AliasShadowingException(ValidationException):
    pass
