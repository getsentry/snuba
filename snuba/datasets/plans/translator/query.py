import copy

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.clickhouse.translators.snuba.mapping import (
    SnubaClickhouseMappingTranslator,
    TranslationMappers,
)
from snuba.query.logical import Query as LogicalQuery


class QueryTranslator:
    """
    Translates a query from the logical representation to the Clickhouse
    one. As of now the Clickhouse representation is only a type alias
    for the Snuba one.

    This relies on the SnubaClickhouseMappingTranslator to transform
    each expression contained in the snuba query.

    A QueryTranslator is supposed to be a stateless component that does
    not mutate the input query and produce a new object as a result.
    """

    def __init__(self, mappers: TranslationMappers) -> None:
        self.__expression_translator = SnubaClickhouseMappingTranslator(mappers)

    def translate(self, query: LogicalQuery) -> ClickhouseQuery:
        translated = ClickhouseQuery(copy.deepcopy(query))
        translated.transform(self.__expression_translator)
        return translated
