from snuba.clickhouse import (
    ColumnSet,
    DateTime,
    LowCardinality,
    Nullable,
    String,
    UInt,
    UUID,
)
from snuba.datasets import DataSet
from snuba.datasets.events_processor import EventsProcessor
from snuba.datasets.schema import ReplacingMergeTreeSchema

class Outcomes(DataSet):
    def __init__(self):
        columns = ColumnSet([
            ('org_id', UInt(64)),
            ('project_id', UInt(64)),
            ('key_id', Nullable(UInt(64))),
            ('timestamp', DateTime()),
            ('outcome', Uint(8)),
            ('reason', LowCardinality(Nullable(String()))),
            ('event_id', Nullable(UUID())),
        ])
