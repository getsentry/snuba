import copy


from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.query.logical import Query as LogicalQuery


class QueryTranslator:
    """
    This is a placeholder interface to identify the component that will translate
    the Logical Query into the Clickhouse Query.

    The implementation will be based on a Visitor pattern (like the formatter) and
    configured with a mapping between the logical schema and the physical schema.
    """

    def translate(self, query: LogicalQuery) -> ClickhouseQuery:
        return copy.deepcopy(query)
