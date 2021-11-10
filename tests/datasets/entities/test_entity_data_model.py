from snuba.clickhouse.columns import Column
from snuba.clickhouse.columns import ColumnSet as PhysicalColumnSet
from snuba.datasets.entities.entity_data_model import EntityColumnSet
from snuba.datasets.entity import convert_to_entity_column_set
from snuba.utils.schemas import (
    FixedString,
    FlattenedColumn,
    String,
    UInt,
    WildcardColumn,
)


def test_entity_data_model() -> None:
    entity_data_model = EntityColumnSet(
        columns=[
            Column("event_id", FixedString(32)),
            Column("project_id", UInt(64)),
            WildcardColumn("tags", String()),
            WildcardColumn("contexts", String()),
        ]
    )

    event_id_col = entity_data_model.get("event_id")
    assert event_id_col is not None
    assert event_id_col.name == "event_id"
    assert event_id_col.type == FixedString(32)

    assert entity_data_model.get("tags[asdf]") == FlattenedColumn(
        None, "tags", String()
    )
    assert entity_data_model.get("asdf") is None
    assert entity_data_model.get("tags[asd   f]") is None
    assert entity_data_model.get("asdf[gkrurrtsjhfkjgh]") is None

    assert entity_data_model == EntityColumnSet(
        columns=[
            Column("event_id", FixedString(32)),
            Column("project_id", UInt(64)),
            WildcardColumn("tags", String()),
            WildcardColumn("contexts", String()),
        ]
    )


def test_convert_column_set() -> None:
    column_set = PhysicalColumnSet(
        [
            Column(name="org_id", type=UInt(64)),
            Column(name="project_id", type=UInt(64)),
        ]
    )

    assert convert_to_entity_column_set(column_set) == EntityColumnSet(
        [Column("org_id", UInt(64)), Column("project_id", UInt(64))]
    )
