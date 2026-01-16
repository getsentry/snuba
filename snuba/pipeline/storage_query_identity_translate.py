from __future__ import annotations

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.plans.storage_processing import get_query_data_source
from snuba.datasets.plans.translator.query import identity_translate
from snuba.datasets.storages.factory import get_storage
from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.simple import Entity, LogicalDataSource, Storage, Table
from snuba.query.data_source.visitor import DataSourceVisitor
from snuba.query.exceptions import InvalidQueryException
from snuba.query.logical import Query as LogicalQuery


def try_translate_storage_query(
    query: CompositeQuery[LogicalDataSource] | LogicalQuery,
) -> CompositeQuery[Table] | ClickhouseQuery | None:
    finder = _LogicalDataSourceFinder()
    finder.visit(query)
    if finder.is_storage_query:
        if not finder.has_join:
            return _translate_storage_query(query)
        else:
            raise InvalidQueryException("Joins not supported for storage queries")
    elif finder.is_mixed_data_source_query:
        raise InvalidQueryException(
            "Queries on storages and entities are not supported",
            data_sources=[str(d) for d in finder.simple_data_sources],
        )
    else:
        # this is not a storage query, pass this on to the rest of the pipeline
        return None


def _translate_storage_query(
    query: CompositeQuery[LogicalDataSource] | LogicalQuery,
) -> CompositeQuery[Table] | ClickhouseQuery:
    # this function assumes there are no joins in storage queries
    from_clause = query.get_from_clause()
    if isinstance(from_clause, Storage) and not isinstance(query, CompositeQuery):
        res = identity_translate(query)
        storage = get_storage(from_clause.key)
        res.set_from_clause(
            get_query_data_source(
                storage.get_schema().get_data_source(),
                allocation_policies=storage.get_allocation_policies(),
                final=query.get_final(),
                sampling_rate=query.get_sample(),
                storage_key=storage.get_storage_key(),
            )
        )
        return res
    else:
        assert isinstance(from_clause, (LogicalQuery, CompositeQuery))
        return CompositeQuery(
            from_clause=_translate_storage_query(from_clause),
            selected_columns=query.get_selected_columns(),
            array_join=query.get_arrayjoin(),
            condition=query.get_condition(),
            groupby=query.get_groupby(),
            having=query.get_having(),
            order_by=query.get_orderby(),
            limitby=query.get_limitby(),
            offset=query.get_offset(),
            totals=query.has_totals(),
            granularity=query.get_granularity(),
        )


class _LogicalDataSourceFinder(
    DataSourceVisitor[
        None,
        LogicalDataSource,
    ],
    JoinVisitor[None, LogicalDataSource],
):
    """Traverses a composite query to understand:

    * what simple data sources exist in the query
    * does the query contain a join

    This is because:
        * Joins are not supported on storage queries (at time of writing (05/05/24)
        * Composite queries where storages and entities are mixed are not supported

    Usage:
        >>> finder = _LogicalDataSourceFinder()
        >>> finder.visit(query)
        >>> assert len(finder.simple_data_sources) == 1
        >>> assert not finder.has_join
    """

    @property
    def is_storage_query(self) -> bool:
        return all((isinstance(d, Storage) for d in self.simple_data_sources))

    @property
    def is_mixed_data_source_query(self) -> bool:
        return any((isinstance(d, Storage) for d in self.simple_data_sources)) and any(
            (isinstance(d, Entity) for d in self.simple_data_sources)
        )

    def __init__(self) -> None:
        self.has_join = False
        self.simple_data_sources: list[LogicalDataSource] = []

    def _visit_simple_source(self, data_source: LogicalDataSource) -> None:
        self.simple_data_sources.append(data_source)

    def _visit_join(self, data_source: JoinClause[LogicalDataSource]) -> None:
        self.has_join = True
        return self.visit_join_clause(data_source)

    def _visit_simple_query(self, data_source: ProcessableQuery[LogicalDataSource]) -> None:
        self.visit(data_source.get_from_clause())

    def _visit_composite_query(self, data_source: CompositeQuery[LogicalDataSource]) -> None:
        self.visit(data_source.get_from_clause())

    def visit_individual_node(self, node: IndividualNode[LogicalDataSource]) -> None:
        self.visit(node.data_source)

    def visit_join_clause(self, node: JoinClause[LogicalDataSource]) -> None:
        node.left_node.accept(self)
        node.right_node.accept(self)
