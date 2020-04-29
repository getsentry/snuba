import copy

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.query.logical import Query as LogicalQuery


class QueryTranslator:
    """
    This is a placeholder interface to identify the component that will translate
    the Logical Query into the Clickhouse Query.

    The implementation will be based on a visitor pattern (like the formatter) and
    configured with a mapping between the logical schema and the physical schema.

    A QueryTranslator is supposed to be a stateless component that does not mutate
    the input query and produce a new object as a result.
    """

    def translate(self, query: LogicalQuery) -> ClickhouseQuery:
        return ClickhouseQuery(copy.deepcopy(query))
