from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.settings import SLICED_KAFKA_TOPIC_MAP


def test_get_physical_topic_name(monkeypatch) -> None:  # type: ignore

    monkeypatch.setitem(
        SLICED_KAFKA_TOPIC_MAP, ("ingest-replay-events", 2), "ingest-replay-events-2"
    )

    storage_key = StorageKey.REPLAYS
    storage = get_writable_storage(storage_key)

    stream_loader = storage.get_table_writer().get_stream_loader()

    default_topic_spec = stream_loader.get_default_topic_spec()

    physical_topic_name = default_topic_spec.get_physical_topic_name(slice_id=2)

    assert physical_topic_name == "ingest-replay-events-2"
