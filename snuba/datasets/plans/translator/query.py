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
        data_source=None,
        selected_columns=query.get_selected_columns_from_ast(),
        array_join=query.get_arrayjoin_from_ast(),
        condition=query.get_condition_from_ast(),
        prewhere=query.get_prewhere_ast(),
        groupby=query.get_groupby_from_ast(),
        having=query.get_having_from_ast(),
        order_by=query.get_orderby_from_ast(),
        limitby=query.get_limitby(),
        sample=query.get_sample(),
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
    """

    def __init__(self, mappers: TranslationMappers) -> None:
        self.__expression_translator = SnubaClickhouseMappingTranslator(mappers)

    def translate(self, query: LogicalQuery) -> ClickhouseQuery:
        translated = identity_translate(query)

        translated.transform(self.__expression_translator)
        return translated
