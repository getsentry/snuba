import importlib
from unittest.mock import patch

from snuba.datasets import table_storage
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey

# define some SLICED_KAFKA_TOPIC_MAP
TEST_SLICED_KAFKA_TOPIC_MAP = {("ingest-replay-events", 2): "ingest-replay-events-2"}


@patch("snuba.settings.SLICED_KAFKA_TOPIC_MAP", TEST_SLICED_KAFKA_TOPIC_MAP)
def test_get_physical_topic_name() -> None:
    importlib.reload(table_storage)

    storage_key = StorageKey("replays")
    storage = get_writable_storage(storage_key)

    stream_loader = storage.get_table_writer().get_stream_loader()

    default_topic_spec = stream_loader.get_default_topic_spec()

    physical_topic_name = default_topic_spec.get_physical_topic_name(slice_id=2)

    assert physical_topic_name == "ingest-replay-events-2"
