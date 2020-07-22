class QueryBuildingException(Exception):
    pass


class ParsingException(QueryBuildingException):
    pass


class ValidationException(QueryBuildingException):
    pass


class CyclicAliasException(ValidationException):
    pass


class AliasShadowingException(ValidationException):
    pass
