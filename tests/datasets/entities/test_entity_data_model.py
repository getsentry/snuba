from snuba.clickhouse.columns import Column
from snuba.clickhouse.columns import ColumnSet as PhysicalColumnSet
from snuba.datasets.entities.entity_data_model import (
    EntityColumnSet,
    FixedString,
    UInt,
    WildcardColumn,
)
from snuba.datasets.entity import convert_to_entity_column_set
from snuba.utils.schemas import FlattenedColumn


def test_entity_data_model() -> None:
    entity_data_model = EntityColumnSet(
        columns=[
            Column("event_id", FixedString(32)),
            Column("project_id", UInt(64)),
            Column("tags", WildcardColumn()),
            Column("contexts", WildcardColumn()),
        ]
    )

    event_id_col = entity_data_model.get("event_id")
    assert event_id_col is not None
    assert event_id_col.name == "event_id"
    assert event_id_col.type == FixedString(32)

    tag_asdf_col = entity_data_model.get("tags[asdf]")
    assert tag_asdf_col is not None

    assert entity_data_model.get("tags[asdf]") == FlattenedColumn(
        None, "tags", WildcardColumn()
    )
    assert entity_data_model.get("asdf") is None
    assert entity_data_model.get("tags[asd   f]") is None
    assert entity_data_model.get("asdf[gkrurrtsjhfkjgh]") is None

    assert entity_data_model == EntityColumnSet(
        columns=[
            Column("event_id", FixedString(32)),
            Column("project_id", UInt(64)),
            Column("tags", WildcardColumn()),
            Column("contexts", WildcardColumn()),
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
