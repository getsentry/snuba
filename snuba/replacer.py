import logging
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from functools import partial
from typing import Callable, List, Mapping, MutableMapping, Optional, Sequence

import simplejson as json
from arroyo import Message
from arroyo.backends.kafka import KafkaPayload
from arroyo.processing.strategies.batching import AbstractBatchWorker

from snuba import settings
from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import (
    ClickhouseClientSettings,
    ClickhouseCluster,
    ClickhouseNode,
)
from snuba.datasets.storage import WritableTableStorage
from snuba.processor import InvalidMessageVersion
from snuba.redis import redis_client
from snuba.replacers.replacer_processor import (
    Replacement,
    ReplacementMessage,
    ReplacementMessageMetadata,
)
from snuba.state import get_config
from snuba.utils.metrics import MetricsBackend
from snuba.utils.rate_limiter import RateLimiter

logger = logging.getLogger("snuba.replacer")

executor = ThreadPoolExecutor()

NODES_REFRESH_PERIOD = 10

RESET_CHECK_CONFIG = "consumer_groups_to_reset_offset_check"


class ShardedConnectionPool(ABC):
    """
    Provides Clickhouse connection to a sharded cluster.

    This class takes care of keeping a list of valid nodes up to date
    and to implement any sort of load balancing strategy.
    """

    @abstractmethod
    def get_connections(self) -> Mapping[int, Sequence[ClickhouseNode]]:
        """
        Returns a sequence of valid connections for each shard.
        The first connection in each sequence is the connection
        that should be used first. The following ones, instead
        are backup in case the first fails.

        The sequences do not have to have the same number of
        connections but that would be weird.
        """
        raise NotImplementedError


class InOrderConnectionPool(ShardedConnectionPool):
    """
    Sharded connection pool that loads the valid nodes from the
    Clickhouse system.clusters table, then provides up to three
    connections for each shard picking them with a round robin
    policy.

    The list of connections provided is always the same so that
    we send all writes to the first node of the shard and only
    use the others as a fallback.

    The rationale is to guarantee in order delivery. If all
    queries are applied on the same node, they are guaranteed to
    be applied in order. If they are applied on random nodes,
    instead, the replication delay can make clickhouse apply
    the updates out of order.
    """

    def __init__(
        self,
        cluster: ClickhouseCluster,
    ) -> None:
        self.__cluster = cluster
        self.__nodes: Mapping[int, List[ClickhouseNode]] = defaultdict(list)
        self.__nodes_refreshed_at = time.time()

    def get_connections(self) -> Mapping[int, Sequence[ClickhouseNode]]:
        now = time.time()

        if not self.__nodes or now - self.__nodes_refreshed_at > NODES_REFRESH_PERIOD:
            all_nodes = self.__cluster.get_local_nodes()

            self.__nodes = defaultdict(list)

            # We need to sort the nodes by replica id as there is no guarantee
            # that `get_local_nodes` will return them sorted. So that we can
            # always hit the same node (the first replica) except during failover.
            valid_nodes = filter(
                lambda node: node.replica is not None and node.shard is not None,
                all_nodes,
            )
            sorted_nodes = sorted(
                valid_nodes,
                # Need to assign a value ro replica if that is None because
                # mypy does not know replica cannot be None at this point.
                key=lambda n: n.replica if n.replica is not None else -1,
            )
            for n in sorted_nodes:
                # mypy does not know we filtered out None values
                assert n.shard is not None
                self.__nodes[n.shard].append(n)

            self.__nodes_refreshed_at = now
        return self.__nodes


class InsertExecutor(ABC):
    """
    Executes the Replacement insert query.

    Each implementation provides a different execution policy.
    """

    @abstractmethod
    def execute(self, replacement: Replacement, record_counts: int) -> int:
        """
        Executes the query according to the policy implemented by this
        class.
        """
        raise NotImplementedError


# Used by InsertExecutors to run the query
RunQuery = Callable[[ClickhousePool, str, int, MetricsBackend], None]


class QueryNodeExecutor(InsertExecutor):
    """
    InsertExecutor that runs one query only on the main query node
    in the cluster. The query can be on a distributed table if the
    cluster has multiple nodes or on a local table if the cluster
    has one node only.
    This relies on Clickhouse to forward the replacement query to
    each storage node.
    """

    def __init__(
        self,
        runner: RunQuery,
        connection: ClickhousePool,
        table: str,
        metrics: MetricsBackend,
    ) -> None:
        self.__connection = connection
        self.__table = table
        self.__metrics = metrics
        self.__runner = runner

    def execute(self, replacement: Replacement, records_count: int) -> int:
        query = replacement.get_insert_query(self.__table)
        if query is None:
            return 0
        self.__runner(self.__connection, query, records_count, self.__metrics)
        return records_count


class ShardedExecutor(InsertExecutor):
    """
    Executes a replacement query on each individual shard in parallel.

    It implements some basic retry logic by trying a different replica
    if the first attempt fails.
    It also falls back on a DistributedExecutor if everything fails.
    """

    def __init__(
        self,
        runner: RunQuery,
        thread_pool: ThreadPoolExecutor,
        cluster: ClickhouseCluster,
        main_connection_pool: ShardedConnectionPool,
        local_table_name: str,
        backup_executor: InsertExecutor,
        metrics: MetricsBackend,
    ) -> None:
        self.__thread_pool = thread_pool
        self.__cluster = cluster
        self.__connection_pool = main_connection_pool
        self.__local_table_name = local_table_name
        self.__backup_executor = backup_executor
        self.__metrics = metrics
        self.__runner = runner

    def __run_multiple_replicas(
        self,
        nodes: Sequence[ClickhouseNode],
        query: str,
        records_count: int,
        metrics: MetricsBackend,
    ) -> None:
        """
        Makes multiple attempts to run the query.
        One per connection provided.
        """
        for remaining_attempts in range(len(nodes), 0, -1):
            try:
                connection = self.__cluster.get_node_connection(
                    ClickhouseClientSettings.REPLACE,
                    nodes[len(nodes) - remaining_attempts],
                )
                self.__runner(
                    connection,
                    query,
                    records_count,
                    metrics,
                )
                return
            except Exception as e:
                if remaining_attempts == 1:
                    raise
                logger.warning(
                    "Replacement processing failed on the main connection",
                    exc_info=e,
                )

    def execute(self, replacement: Replacement, records_count: int) -> int:
        try:
            query = replacement.get_insert_query(self.__local_table_name)
            if query is None:
                return 0
            result_futures = []
            for nodes in self.__connection_pool.get_connections().values():
                result_futures.append(
                    self.__thread_pool.submit(
                        partial(
                            self.__run_multiple_replicas,
                            nodes=nodes,
                            query=query,
                            records_count=records_count,
                            metrics=self.__metrics,
                        )
                    )
                )
            for result in as_completed(result_futures):
                e = result.exception()
                if e is not None:
                    raise e
            return records_count

        except Exception as e:
            count = self.__backup_executor.execute(replacement, records_count)
            logger.warning(
                "Replacement processing failed on the main connection",
                exc_info=e,
            )
            return count


class ReplacerWorker(AbstractBatchWorker[KafkaPayload, Replacement]):
    def __init__(
        self,
        storage: WritableTableStorage,
        consumer_group: str,
        metrics: MetricsBackend,
    ) -> None:
        self.__storage = storage

        self.metrics = metrics
        processor = storage.get_table_writer().get_replacer_processor()
        assert (
            processor
        ), f"This storage writer does not support replacements {storage.get_storage_key().value}"
        self.__replacer_processor = processor
        self.__database_name = storage.get_cluster().get_database()

        self.__sharded_pool = InOrderConnectionPool(self.__storage.get_cluster())
        self.__rate_limiter = RateLimiter("replacements")

        self.__last_offset_processed_per_partition: MutableMapping[str, int] = dict()
        self.__consumer_group = consumer_group

    def __get_insert_executor(self, replacement: Replacement) -> InsertExecutor:
        """
        Some replacements need to be executed on each storage node of the
        cluster instead that through a query node on distributed tables.
        This happens when the number of shards changes and specific rows
        resolve to a different node than they were doing before.

        example: writing the tombstone for an event id. When we change the
        shards number the tombstone may end up on a different shard than
        the original row.

        This returns the InsertExecutor that implements the appropriate
        policy for the replacement provided. So it can return either a basic
        DistributedExecutor or a ShardedExecutor to write on each storage
        node.
        """

        def run_query(
            connection: ClickhousePool,
            query: str,
            records_count: int,
            metrics: MetricsBackend,
        ) -> None:
            t = time.time()

            logger.debug("Executing replace query: %s" % query)
            connection.execute_robust(query)
            duration = int((time.time() - t) * 1000)

            logger.info("Replacing %s rows took %sms" % (records_count, duration))
            metrics.timing(
                "replacements.count",
                records_count,
                tags={"host": connection.host},
            )
            metrics.timing(
                "replacements.duration",
                duration,
                tags={"host": connection.host},
            )

        query_table_name = self.__replacer_processor.get_schema().get_table_name()
        local_table_name = self.__replacer_processor.get_schema().get_local_table_name()
        cluster = self.__storage.get_cluster()

        query_connection = cluster.get_query_connection(
            ClickhouseClientSettings.REPLACE
        )
        write_every_node = replacement.should_write_every_node()
        query_node_executor = QueryNodeExecutor(
            runner=run_query,
            connection=query_connection,
            table=query_table_name,
            metrics=self.metrics,
        )
        if not write_every_node or cluster.is_single_node():
            return query_node_executor

        return ShardedExecutor(
            runner=run_query,
            cluster=cluster,
            thread_pool=executor,
            main_connection_pool=self.__sharded_pool,
            local_table_name=local_table_name,
            backup_executor=query_node_executor,
            metrics=self.metrics,
        )

    def process_message(self, message: Message[KafkaPayload]) -> Optional[Replacement]:
        metadata = ReplacementMessageMetadata(
            partition_index=message.partition.index,
            offset=message.offset,
            consumer_group=self.__consumer_group,
        )

        if self._message_already_processed(metadata):
            logger.warning(
                f"Replacer ignored a message, consumer group: {self.__consumer_group}",
                extra={
                    "partition": metadata.partition_index,
                    "offset": metadata.offset,
                },
            )
            if get_config("skip_seen_offsets", False):
                return None
        seq_message = json.loads(message.payload.value)
        [version, action_type, data] = seq_message

        if version == 2:
            return self.__replacer_processor.process_message(
                ReplacementMessage(
                    action_type=action_type,
                    data=data,
                    metadata=metadata,
                )
            )
        else:
            raise InvalidMessageVersion("Unknown message format: " + str(seq_message))

    def flush_batch(self, batch: Sequence[Replacement]) -> None:
        need_optimize = False
        clickhouse_read = self.__storage.get_cluster().get_query_connection(
            ClickhouseClientSettings.REPLACE
        )

        for replacement in batch:

            start_time = time.time()

            table_name = self.__replacer_processor.get_schema().get_table_name()
            count_query = replacement.get_count_query(table_name)

            if count_query is not None:
                count = clickhouse_read.execute_robust(count_query).results[0][0]
                if count == 0:
                    continue
            else:
                count = 0

            need_optimize = (
                self.__replacer_processor.pre_replacement(replacement, count)
                or need_optimize
            )

            query_executor = self.__get_insert_executor(replacement)
            with self.__rate_limiter as state:
                self.metrics.increment("insert_state", tags={"state": state[0].value})
                count = query_executor.execute(replacement, count)

            self.__replacer_processor.post_replacement(replacement, count)

            self._check_timing_and_write_to_redis(replacement, start_time)

        if need_optimize:
            from snuba.optimize import run_optimize

            today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            num_dropped = run_optimize(
                clickhouse_read, self.__storage, self.__database_name, before=today
            )
            logger.info(
                "Optimized %s partitions on %s" % (num_dropped, clickhouse_read.host)
            )

    def _message_already_processed(self, metadata: ReplacementMessageMetadata) -> bool:
        """
        Figure out whether or not the message was already processed.

        Check if there exists a recorded offset that took too long to execute.
        If there is, we can conclude whether or not to process the incoming
        message by comparing the incoming message to the recorded offset.
        """
        key = self._build_topic_group_index_key(metadata)
        self._reset_offset_check(key)

        if key not in self.__last_offset_processed_per_partition:
            processed_offset = redis_client.get(key)
            try:
                self.__last_offset_processed_per_partition[key] = (
                    -1 if processed_offset is None else int(processed_offset)
                )
            except ValueError as e:
                redis_client.delete(key)
                logger.warning(
                    "Unexpected value found for an offset in Redis",
                    exc_info=e,
                )
                self.__last_offset_processed_per_partition[key] = -1

        return metadata.offset <= self.__last_offset_processed_per_partition[key]

    def _reset_offset_check(self, key: str) -> None:
        """
        We may need to clear the offset the replacer is comparing incoming messages
        against.

        Eg. The offset is manually reset in Kafka to start processing messages
        again from an older offset. The replacer will ignore this and just skip
        messages till it's back to the offset stored in Redis.

        There exists a config which tells us which consumer groups require their
        replacer(s) reset. Ideally this config is populated with consumer groups
        temporarily, then cleared once relevant consumers restart.
        """
        # expected format is "[consumer_group1,consumer_group2,..]"
        consumer_groups = (get_config(RESET_CHECK_CONFIG) or "[]")[1:-1].split(",")
        if self.__consumer_group in consumer_groups:
            self.__last_offset_processed_per_partition[key] = -1
            redis_client.delete(key)

    def _check_timing_and_write_to_redis(
        self, replacement: Replacement, start_time: float
    ) -> None:
        """
        Write the offset just processed to Redis if execution took longer than the threshold.
        Also store the offset locally to avoid Read calls to Redis.

        If the Consumer dies while the insert query for the message was being executed,
        the most recently executed offset would be present in Redis.
        """
        if (time.time() - start_time) < settings.REPLACER_PROCESSING_TIMEOUT_THRESHOLD:
            return
        message_metadata = replacement.get_message_metadata()
        key = self._build_topic_group_index_key(message_metadata)
        redis_client.set(
            key,
            message_metadata.offset,
            ex=settings.REPLACER_PROCESSING_TIMEOUT_THRESHOLD_KEY_TTL,
        )
        self.__last_offset_processed_per_partition[key] = message_metadata.offset

    def _build_topic_group_index_key(
        self, message_metadata: ReplacementMessageMetadata
    ) -> str:
        """
        Builds a unique key for a message being processed for a specific
        consumer group and partition.
        """
        return ":".join(
            [
                "replacement",
                self.__consumer_group,
                self.__replacer_processor.get_state().value,
                str(message_metadata.partition_index),
            ]
        )
