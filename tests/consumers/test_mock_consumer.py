from datetime import datetime

from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.processing.strategies.streaming.factory import KafkaConsumerStrategyFactory
from arroyo.types import Message, Partition, Topic

from snuba.clickhouse.formatter.nodes import FormattedQuery, StringNode
from snuba.consumers.consumer import build_mock_batch_writer
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from tests.backends.metrics import TestingMetricsBackend


def test_mock_consumer() -> None:
    storage = get_writable_storage(StorageKey.ERRORS)

    strategy = KafkaConsumerStrategyFactory(
        None,
        lambda message: None,
        build_mock_batch_writer(storage, True, TestingMetricsBackend(), 100, 50),
        max_batch_size=1,
        max_batch_time=1,
        processes=None,
        input_block_size=None,
        output_block_size=None,
        initialize_parallel_transform=None,
    ).create_with_partitions(lambda message: None, {})

    strategy.submit(
        Message(
            Partition(Topic("events"), 0),
            1,
            KafkaPayload(None, b"INVALID MESSAGE", []),
            datetime.now(),
        )
    )
    strategy.close()
    strategy.join()

    # If the mock was not applied correctly we would have data in Clickhouse
    reader = storage.get_cluster().get_reader()
    result = reader.execute(
        FormattedQuery([StringNode("SELECT count() as c from errors_local")])
    )
    assert result["data"] == [{"c": 0}]
