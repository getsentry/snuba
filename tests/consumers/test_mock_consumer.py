from datetime import datetime
from unittest.mock import Mock

from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.types import BrokerValue, Message, Partition, Topic

from snuba.clickhouse.formatter.nodes import FormattedQuery, StringNode
from snuba.consumers.consumer import build_mock_batch_writer
from snuba.consumers.strategy_factory import KafkaConsumerStrategyFactory
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from tests.backends.metrics import TestingMetricsBackend


def test_mock_consumer() -> None:
    storage = get_writable_storage(StorageKey.ERRORS)

    commit = Mock()

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
    ).create_with_partitions(commit, {})

    strategy.submit(
        Message(
            BrokerValue(
                KafkaPayload(None, b"INVALID MESSAGE", []),
                Partition(Topic("events"), 0),
                1,
                datetime.now(),
            )
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
