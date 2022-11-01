from snuba.datasets.configuration.utils import serialize_columns
from snuba.datasets.storages.factory import initialize_storage_factory

initialize_storage_factory()
from snuba.datasets.storages.generic_metrics import sets_storage

GEN_METRICS_COLUMNS = [
    {"name": "org_id", "type": "UInt", "args": {"size": 64}},
    {"name": "use_case_id", "type": "String"},
    {"name": "project_id", "type": "UInt", "args": {"size": 64}},
    {"name": "metric_id", "type": "UInt", "args": {"size": 64}},
    {"name": "timestamp", "type": "DateTime"},
    {"name": "retention_days", "type": "UInt", "args": {"size": 16}},
    {
        "name": "tags",
        "type": "Nested",
        "args": {
            "subcolumns": [
                {"name": "key", "type": "UInt", "args": {"size": 64}},
                {"name": "indexed_value", "type": "UInt", "args": {"size": 64}},
                {"name": "raw_value", "type": "String"},
            ],
        },
    },
    {
        "name": "_raw_tags_hash",
        "type": "Array",
        "args": {"type": "UInt", "arg": 64, "schema_modifiers": ["readonly"]},
    },
    {
        "name": "_indexed_tags_hash",
        "type": "Array",
        "args": {"type": "UInt", "arg": 64, "schema_modifiers": ["readonly"]},
    },
    {"name": "granularity", "type": "UInt", "args": {"size": 8}},
    {
        "name": "value",
        "type": "AggregateFunction",
        "args": {"func": "uniqCombined64", "arg_types": [{"type": "UInt", "arg": 64}]},
    },
]


def test_serialize_columns() -> None:
    print(serialize_columns(sets_storage))
    # assert serialize_columns(sets_storage) == GEN_METRICS_COLUMNS
