from typing import Sequence

from snuba.clickhouse.query import Query
from snuba.clickhouse.query_dsl.accessors import get_object_ids_in_query_ast
from snuba.query.expressions import Expression, Literal
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import QuerySettings


class LiteralRewriter(ClickhouseQueryProcessor):
    """
    Removes any string literals by value and replaces them with another value,
    for the given project IDs.

    Example: tags["myTag"]
        -> arrayElement("tags.value", indexOf("tags.key", "myTag"))
        -> arrayElement("tags.value", indexOf("tags.key", "myOtherTag"))
        -> toString(promoted_MyTag)

        literal_mappings = {
            "myTag": "myOtherTag"
        }


    This happens if there is a promoted_MyTag column in the storage
    that maps to the myTag tag.
    """

    def __init__(
        self,
        project_column: str,
        literals: Sequence[str],
    ) -> None:
        self.__project_column = project_column
        self.__literals = set(literals)
        from snuba import settings

        self.__project_ids = set(settings.SNUBA_SPANS_USER_IP_PROJECTS)
        self.__new_value = settings.SNUBA_SPANS_USER_IP_NEW_VALUE

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        project_ids = set(get_object_ids_in_query_ast(query, self.__project_column))

        if not project_ids & self.__project_ids:
            return

        def transform_literal(exp: Expression) -> Expression:
            if not isinstance(exp, Literal):
                return exp

            if not isinstance(exp.value, str):
                return exp

            if exp.value not in self.__literals:
                return exp

            return Literal(alias=exp.alias, value=self.__new_value)

        query.transform_expressions(transform_literal)
