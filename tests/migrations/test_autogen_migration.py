import pytest
import yaml

from snuba.clickhouse.columns import Column
from snuba.migrations.autogeneration.autogen_migrations import get_added_columns
from snuba.utils.schemas import SchemaModifiers, UInt


@pytest.fixture
def mockstorageyaml() -> str:
    return """
version: v1
kind: writable_storage
name: errors
storage:
    key: errors
    set_key: events
readiness_state: complete
schema:
    columns:
        [
            { name: project_id, type: UInt, args: { size: 64 } },
            { name: timestamp, type: DateTime },
            { name: event_id, type: UUID }
        ]
    local_table_name: errors_local
    dist_table_name: errors_dist
    partition_format:
        - retention_days
        - date
    not_deleted_mandatory_condition: deleted
"""


def test_get_new_columns(mockstorageyaml: str) -> None:
    new_storage = yaml.safe_load(mockstorageyaml)
    new_storage["schema"]["columns"].append(
        {
            "name": "purple_melon",
            "type": "UInt",
            "args": {"schema_modifiers": ["readonly"], "size": 8},
        },
    )
    cols = get_added_columns(mockstorageyaml, yaml.dump(new_storage))
    expected = [
        Column(
            name="purple_melon", type=UInt(8, modifiers=SchemaModifiers(readonly=True))
        )
    ]
    assert cols == expected
