from typing import Mapping, Optional

from snuba.clickhouse.query import Query
from snuba.clickhouse.query_dsl.accessors import get_object_ids_in_query_ast
from snuba.query.expressions import Expression, Literal
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import QuerySettings


class LiteralRewriter(ClickhouseQueryProcessor):
    """
    Removes any string literals by value and replaces them with another value,
    for the given (optional) project IDs. If no project IDs are provided, all
    projects will be matched.

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
        literal_mappings: Mapping[str, str],
        project_ids_setting_key: Optional[str],
    ) -> None:
        self.__project_column = project_column
        self.__literal_mappings = literal_mappings
        from snuba import settings

        if project_ids_setting_key is None:
            self.__project_ids = None
        else:
            self.__project_ids = getattr(settings, project_ids_setting_key)

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        project_ids = get_object_ids_in_query_ast(query, self.__project_column)

        if self.__project_ids is not None:
            if not project_ids & self.__project_ids:
                return

        def transform_literal(exp: Expression) -> Expression:
            if not isinstance(exp, Literal):
                return exp

            if not isinstance(exp.value, str):
                return exp

            try:
                return Literal(
                    alias=exp.alias, value=self.__literal_mappings[exp.value]
                )
            except KeyError:
                return exp

        query.transform_expressions(transform_literal)
