from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.clickhouse.translators.snuba.mapping import (
    SnubaClickhouseMappingTranslator,
    TranslationMappers,
)
from snuba.query.logical import Query as LogicalQuery


def identity_translate(query: LogicalQuery) -> ClickhouseQuery:
    """
    Utility method to build a Clickhouse Query from a Logical Query
    without transforming anything.

    It is exposed by this module because it is often useful in tests.
    """
    return ClickhouseQuery(
        from_clause=None,
        selected_columns=query.get_selected_columns(),
        array_join=query.get_arrayjoin(),
        condition=query.get_condition(),
        groupby=query.get_groupby(),
        having=query.get_having(),
        order_by=query.get_orderby(),
        limitby=query.get_limitby(),
        limit=query.get_limit(),
        offset=query.get_offset(),
        totals=query.has_totals(),
        granularity=query.get_granularity(),
    )


class QueryTranslator:
    """
    Translates a query from the logical representation to the Clickhouse
    one. As of now the Clickhouse representation is only a type alias
    for the Snuba one.

    This relies on the SnubaClickhouseMappingTranslator to transform
    each expression contained in the snuba query.

    A QueryTranslator is supposed to be a stateless component that does
    not mutate the input query and produce a new object as a result.

    The QueryTranslator class should only be used once per query, since the
    underlying translator has a cache.
    """

    def __init__(self, mappers: TranslationMappers) -> None:
        self.__expression_translator = SnubaClickhouseMappingTranslator(mappers)

    def translate(self, query: LogicalQuery) -> ClickhouseQuery:
        translated = identity_translate(query)

        translated.transform(self.__expression_translator)
        return translated
