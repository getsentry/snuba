from typing import Optional, Set

from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.simple import Table
from snuba.query.data_source.visitor import DataSourceVisitor


class TablesCollector(DataSourceVisitor[None, Table], JoinVisitor[None, Table]):
    """
    Traverses the data source of a composite query and collects
    all the referenced table names, final state and sampling rate
    to fill stats.
    """

    def __init__(self) -> None:
        self.__tables: Set[str] = set()
        self.__final: bool = False
        self.__sample_rate: Optional[float] = None

    def get_tables(self) -> Set[str]:
        return self.__tables

    def any_final(self) -> bool:
        return self.__final

    def get_sample_rate(self) -> Optional[float]:
        return self.__sample_rate

    def _visit_simple_source(self, data_source: Table) -> None:
        self.__tables.add(data_source.table_name)
        self.__sample_rate = data_source.sampling_rate
        if data_source.final:
            self.__final = True

    def _visit_join(self, data_source: JoinClause[Table]) -> None:
        self.visit_join_clause(data_source)

    def _visit_simple_query(self, data_source: ProcessableQuery[Table]) -> None:
        self.visit(data_source.get_from_clause())

    def _visit_composite_query(self, data_source: CompositeQuery[Table]) -> None:
        self.visit(data_source.get_from_clause())
        # stats do not yet support sampling rate (there is only one field)
        # so if we have a composite query we set it to None.
        self.__sample_rate = None

    def visit_individual_node(self, node: IndividualNode[Table]) -> None:
        self.visit(node.data_source)

    def visit_join_clause(self, node: JoinClause[Table]) -> None:
        node.left_node.accept(self)
        node.right_node.accept(self)
