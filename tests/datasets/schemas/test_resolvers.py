from snuba.clickhouse.columns import ColumnSet, Nested, String, UInt
from snuba.datasets.schemas.join import JoinClause, JoinType, TableJoinNode
from snuba.datasets.schemas.resolver import (
    JoinedTablesResolver,
    ResolvedCol,
    SingleTableResolver,
)


def test_single_column_set_resolver() -> None:
    col_set = ColumnSet(
        [
            ("col1", String()),
            ("col_2", UInt(8)),
            ("tags", Nested([("key", String()), ("value", String())])),
        ]
    )

    resolver = SingleTableResolver(col_set, ["tags_key", "tags_value"], "table")
    assert resolver.resolve_column("col1") == ResolvedCol(
        table_name="table", column_name="col1", path=[]
    )
    assert resolver.resolve_column("col_2") == ResolvedCol(
        table_name="table", column_name="col_2", path=[]
    )
    assert resolver.resolve_column("tags") == ResolvedCol(
        table_name="table", column_name="tags", path=[]
    )
    assert resolver.resolve_column("tags.value") == ResolvedCol(
        table_name="table", column_name="tags", path=["value"]
    )
    assert resolver.resolve_column("tags_value") == ResolvedCol(
        table_name="table", column_name="tags_value", path=[]
    )
    assert resolver.resolve_column("something_else") is None


def test_joined_column_set_resolver() -> None:
    join_root = JoinClause(
        left_node=TableJoinNode(
            table_name="table1",
            columns=ColumnSet(
                [
                    ("col1", String()),
                    ("col_2", UInt(8)),
                    ("tags", Nested([("key", String()), ("value", String())])),
                ]
            ),
            mandatory_conditions=[],
            prewhere_candidates=[],
            alias="table1",
        ),
        right_node=TableJoinNode(
            table_name="table2",
            columns=ColumnSet(
                [
                    ("col1", String()),
                    ("tags", Nested([("key", String()), ("value", String())])),
                ]
            ),
            mandatory_conditions=[],
            prewhere_candidates=[],
            alias="table2",
        ),
        mapping=[],
        join_type=JoinType.LEFT,
    )

    resolver = JoinedTablesResolver(
        join_root=join_root,
        virtual_column_names={"table1": ["tags_key", "tags_value"]},
    )

    assert resolver.resolve_column("col1") is None
    assert resolver.resolve_column("table1.col1") == ResolvedCol(
        table_name="table1", column_name="col1", path=[]
    )
    assert resolver.resolve_column("table2.col1") == ResolvedCol(
        table_name="table2", column_name="col1", path=[]
    )
    assert resolver.resolve_column("table2.col_2") is None
    assert resolver.resolve_column("table1.col_2") == ResolvedCol(
        table_name="table1", column_name="col_2", path=[]
    )
    assert resolver.resolve_column("table1.tags") == ResolvedCol(
        table_name="table1", column_name="tags", path=[]
    )
    assert resolver.resolve_column("table1.tags.key") == ResolvedCol(
        table_name="table1", column_name="tags", path=["key"]
    )
    assert resolver.resolve_column("table1.tags_value") == ResolvedCol(
        table_name="table1", column_name="tags_value", path=[]
    )
    assert resolver.resolve_column("table2.tags_value") is None
    assert resolver.resolve_column("table1.something_else") is None
