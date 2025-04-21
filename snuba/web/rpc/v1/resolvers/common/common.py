from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import LogicalDataSource
from snuba.query.logical import Query
from snuba.query.parser import validate_aliases as validate_aliases_parser
from snuba.query.parser.exceptions import AliasShadowingException
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException


def validate_aliases(query: CompositeQuery[LogicalDataSource] | Query) -> None:
    """
    raises BadSnubaRPCRequestException if a query has duplicate aliases
    """
    try:
        validate_aliases_parser(query)
    except AliasShadowingException as e:
        raise BadSnubaRPCRequestException("Duplicate labels in request") from e
