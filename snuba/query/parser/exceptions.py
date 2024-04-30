from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity, Storage
from snuba.query.exceptions import InvalidQueryException, ValidationException
from snuba.query.logical import Query, StorageQuery


class ParsingException(InvalidQueryException):
    pass


class CyclicAliasException(ValidationException):
    pass


class AliasShadowingException(ValidationException):
    pass


class PostProcessingError(Exception):
    """
    Class for exceptions that happen during post processing of a query,
    after the original query has been created
    """

    def __init__(
        self,
        query: Query | StorageQuery | CompositeQuery[Entity] | CompositeQuery[Storage],
        message: str | None = None,
    ):
        super().__init__(message)
        self.query = query
