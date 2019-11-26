from snuba.datasets.schemas.join import JoinClause
from snuba.query.columns import QUALIFIED_COLUMN_REGEX
from snuba.query.query import Query
from snuba.query.query_processor import QueryProcessor
from snuba.request.request_settings import RequestSettings


class SimpleJoinOptimizer(QueryProcessor):
    """
    Simplest possible join optimizer. It turns a join expression into a single
    table expression if only one table is referenced in the query.
    At this stage this is basically a proof of concept, we can build
    a more sophisticated optimization based on this.

    TODO: Optimize a join between multiple tables by minimizing the number
    of tables joined together when more than one is referenced in the query.
    """

    def process_query(self, query: Query, request_settings: RequestSettings,) -> None:
        from_clause = query.get_data_source()
        if not isinstance(from_clause, JoinClause):
            return

        referenced_columns = query.get_all_referenced_columns()
        referenced_aliases = set()
        for qualified_column in referenced_columns:
            # This will be much better when we will represent columns
            # with a more structured data type than strings.
            match = QUALIFIED_COLUMN_REGEX.match(qualified_column)
            if match:
                # match[1] is the first parenthesized group in the regex, thus
                # the table alias.
                table_alias = match[1]
                referenced_aliases.add(table_alias)

        assert (
            len(referenced_aliases) > 0
        ), "Trying to otpimize a join query without aliases"
        if len(referenced_aliases) > 1:
            return

        from_tables = from_clause.get_tables()
        table = from_tables[referenced_aliases.pop()]

        query.set_data_source(table)
