from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import LogicalDataSource
from snuba.query.logical import Query
from snuba.query.parser import validate_aliases as validate_aliases_parser
from snuba.query.parser.exceptions import AliasShadowingException
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException


def validate_aliases(query: CompositeQuery[LogicalDataSource] | Query) -> None:
    try:
        validate_aliases_parser(query)
    except AliasShadowingException as e:
        raise BadSnubaRPCRequestException("duplicate labels detected") from e
