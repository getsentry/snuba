import logging
import time
from datetime import datetime
from typing import Optional, Sequence, Tuple

import simplejson as json
from arroyo import Message
from arroyo.backends.kafka import KafkaPayload
from arroyo.processing.strategies.batching import AbstractBatchWorker

from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storage import WritableTableStorage
from snuba.processor import InvalidMessageVersion
from snuba.replacers.replacer_processor import Replacement, ReplacementMessage
from snuba.utils.metrics import MetricsBackend

logger = logging.getLogger("snuba.replacer")


class ReplacerWorker(AbstractBatchWorker[KafkaPayload, Replacement]):
    def __init__(self, storage: WritableTableStorage, metrics: MetricsBackend) -> None:
        self.__storage = storage

        self.metrics = metrics
        processor = storage.get_table_writer().get_replacer_processor()
        assert (
            processor
        ), f"This storage writer does not support replacements {storage.get_storage_key().value}"
        self.__replacer_processor = processor
        self.__database_name = storage.get_cluster().get_database()

    def __get_write_connections(
        self, replacement: Replacement
    ) -> Tuple[str, Sequence[ClickhousePool]]:
        """
        Some replacements need to be executed on each storage node of the
        cluster instead that through a query node on distributed tables.
        This happens when the number of shards changes and specific rows
        resolve to a different node than they were doing before.

        example: writing the tombstone for an event id. When we change the
        shards number the tombstone may end up on a different shard than
        the original row.

        This returns connections to one replica for each shard so that,
        in these cases, we can run the INSERT on each node.
        """
        query_table_name = self.__replacer_processor.get_schema().get_table_name()
        local_table_name = self.__replacer_processor.get_schema().get_local_table_name()
        cluster = self.__storage.get_cluster()

        write_every_node = replacement.write_every_node()
        if not write_every_node or cluster.is_single_node():
            return (
                query_table_name,
                [cluster.get_query_connection(ClickhouseClientSettings.REPLACE)],
            )

        return (
            local_table_name,
            [
                cluster.get_node_connection(ClickhouseClientSettings.REPLACE, node)
                for node in self.__storage.get_cluster().get_local_nodes()
                if node.replica == 1
            ],
        )

    def process_message(self, message: Message[KafkaPayload]) -> Optional[Replacement]:
        seq_message = json.loads(message.payload.value)
        version = seq_message[0]

        if version == 2:
            return self.__replacer_processor.process_message(
                ReplacementMessage(seq_message[1], seq_message[2])
            )
        else:
            raise InvalidMessageVersion("Unknown message format: " + str(seq_message))

    def flush_batch(self, batch: Sequence[Replacement]) -> None:
        need_optimize = False
        clickhouse_read = self.__storage.get_cluster().get_query_connection(
            ClickhouseClientSettings.REPLACE
        )
        for replacement in batch:
            table_name = self.__replacer_processor.get_schema().get_table_name()
            count_query = replacement.get_count_query(table_name)

            if count_query is not None:
                count = clickhouse_read.execute_robust(count_query)[0][0]
                if count == 0:
                    continue
            else:
                count = 0

            need_optimize = (
                self.__replacer_processor.pre_replacement(replacement, count)
                or need_optimize
            )

            write_table_name, clickhouse_write = self.__get_write_connections(
                replacement
            )
            insert_query = replacement.get_insert_query(write_table_name)

            if insert_query is not None:
                for connection in clickhouse_write:
                    t = time.time()
                    logger.debug("Executing replace query: %s" % insert_query)
                    connection.execute_robust(insert_query)
                    duration = int((time.time() - t) * 1000)

                    logger.info("Replacing %s rows took %sms" % (count, duration))
                    self.metrics.timing("replacements.count", count)
                    self.metrics.timing("replacements.duration", duration)
            else:
                count = duration = 0

            self.__replacer_processor.post_replacement(replacement, duration, count)

        if need_optimize:
            from snuba.optimize import run_optimize

            today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            num_dropped = run_optimize(
                clickhouse_read, self.__storage, self.__database_name, before=today,
            )
            logger.info(
                "Optimized %s partitions on %s" % (num_dropped, clickhouse_read.host)
            )
