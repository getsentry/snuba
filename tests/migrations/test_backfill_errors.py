from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.migrations.backfill_errors import backfill_errors
from tests.fixtures import get_raw_event
from tests.helpers import write_unprocessed_events


def test_backfill_errors() -> None:
    errors_storage = get_writable_storage(StorageKey.ERRORS)
    clickhouse = errors_storage.get_cluster().get_query_connection(
        ClickhouseClientSettings.QUERY
    )
    errors_table_name = errors_storage.get_table_writer().get_schema().get_table_name()

    def get_errors_count() -> int:
        return clickhouse.execute(f"SELECT count() from {errors_table_name}")[0][0]

    raw_events = []
    for i in range(10):
        event = get_raw_event()
        raw_events.append(event)

    events_storage = get_writable_storage(StorageKey.EVENTS)

    write_unprocessed_events(events_storage, raw_events)

    assert get_errors_count() == 0

    backfill_errors()

    assert get_errors_count() == 10

    assert clickhouse.execute(
        f"SELECT contexts.key, contexts.value from {errors_table_name} LIMIT 1;"
    )[0] == (
        (
            "device_model_id",
            "geo_city",
            "geo_country_code",
            "geo_region",
            "os_kernel_version",
        ),
        ("Galaxy", "San Francisco", "US", "CA", "1.1.1"),
    )
