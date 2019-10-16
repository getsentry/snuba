import re

from snuba.datasets.schemas.join import JoinClause
from snuba.query.columns import all_referenced_columns
from snuba.query.query import Query
from snuba.query.query_processor import QueryProcessor
from snuba.request.request_settings import RequestSettings

QUALIFIED_COLUMN_REGEX = re.compile(r"^([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z0-9_\.\[\]]+)$")


class SimpleJoinOptimizer(QueryProcessor):
    def process_query(self,
        query: Query,
        request_settings: RequestSettings,
    ) -> None:
        from_clause = query.get_data_source()
        if not isinstance(from_clause, JoinClause):
            return

        referenced_columns = all_referenced_columns(query)
        referenced_aliases = set()
        for qualified_column in referenced_columns:
            match = QUALIFIED_COLUMN_REGEX.match(qualified_column)
            if match:
                table_alias = match[1]
                referenced_aliases.add(table_alias)

        if len(referenced_aliases) != 1:
            # Assume we need a join we cannot collapse this as a table
            # If len(referenced_aliases) is 0 we should never get here.
            return

        from_tables = from_clause.get_tables()
        table = from_tables[referenced_aliases.pop()]

        query.set_data_source(table)
