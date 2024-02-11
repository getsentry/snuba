from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity
from snuba.query.logical import Query
from snuba.utils.serializable_exception import SerializableException


class InvalidJsonRequestException(SerializableException):
    """
    Common parent class for exceptions signaling the json payload
    of the request was not valid.
    """

    pass


class JsonDecodeException(InvalidJsonRequestException):
    pass


class JsonSchemaValidationException(InvalidJsonRequestException):
    pass


class PostProcessingError(Exception):
    """
    Class for exceptions that happen during post processing of a query,
    after the original query has been created
    """

    def __init__(
        self,
        query: Query | CompositeQuery[Entity],
        snql_anonymized: str,
        e: Exception,
        message: str | None = None,
    ):
        super().__init__(message)
        self.query = query
        self.snql_anonymized = snql_anonymized
        self.e = e
