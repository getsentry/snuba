from snuba.clickhouse.columns import Column
from snuba.clickhouse.columns import ColumnSet as PhysicalColumnSet
from snuba.clickhouse.columns import FlattenedColumn
from snuba.clickhouse.columns import UInt as PhysicalUInt
from snuba.datasets.entities.entity_data_model import (
    EntityColumnSet,
    FixedString,
    String,
    UInt,
    WildcardColumn,
)
from snuba.datasets.entity import convert_to_entity_column_set


def test_entity_data_model() -> None:
    entity_data_model = EntityColumnSet(
        columns=[
            Column("event_id", FixedString(32)),
            Column("project_id", UInt(64)),
            WildcardColumn("tags", String()),
            WildcardColumn("contexts", String()),
        ]
    )

    assert entity_data_model.get("tags[asdf]") == WildcardColumn("tags", String())
    assert entity_data_model.get("event_id") == Column("event_id", FixedString(32))
    assert entity_data_model.get("asdf") is None
    assert entity_data_model.get("tags[asd   f]") is None
    assert entity_data_model.get("asdf[gkrurrtsjhfkjgh]") is None


def test_convert_column_set() -> None:
    column_set = PhysicalColumnSet(
        [
            FlattenedColumn(name="org_id", type=PhysicalUInt(64)),
            FlattenedColumn(name="project_id", type=PhysicalUInt(64)),
        ]
    )

    assert convert_to_entity_column_set(column_set) == EntityColumnSet(
        [Column("org_id", UInt(64)), Column("project_id", UInt(64))]
    )
