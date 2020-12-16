from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.migrations.backfill_errors import backfill_errors
from tests.fixtures import get_raw_event
from tests.helpers import write_unprocessed_events


def get_errors_count() -> int:
    errors_storage = get_writable_storage(StorageKey.ERRORS)
    errors_table_name = errors_storage.get_table_writer().get_schema().get_table_name()
    clickhouse = errors_storage.get_cluster().get_query_connection(
        ClickhouseClientSettings.QUERY
    )
    return clickhouse.execute(f"SELECT count() from {errors_table_name}")[0][0]


def test_backfill_errors() -> None:
    events_storage = get_writable_storage(StorageKey.EVENTS)

    write_unprocessed_events(events_storage, [get_raw_event()])

    assert get_errors_count() == 0

    backfill_errors()

    assert get_errors_count() == 1
