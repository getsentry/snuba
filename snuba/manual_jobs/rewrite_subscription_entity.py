from snuba.datasets.entities.entity_key import EntityKey
from snuba.manual_jobs import Job, JobLogger, JobSpec
from snuba.redis import RedisClientKey, get_redis_client
from snuba.subscriptions.data import PartitionId
from snuba.subscriptions.store import RedisSubscriptionDataStore
from snuba.utils.metrics.wrapper import MetricsWrapper

redis_client = get_redis_client(RedisClientKey.SUBSCRIPTION_STORE)


class RewriteSubscriptionEntity(Job):
    """
    A manual job to rewrite the entity field in subscriptions table.
    """

    def __init__(self, job_spec: JobSpec) -> None:
        super().__init__(job_spec)
        self.__metrics = MetricsWrapper(None)

    def execute(self, logger: JobLogger) -> None:
        """
        Executes the job to rewrite subscription entities.
        """
        partitions = redis_client.keys("subscriptions:{entity.value}:*")
        for partition_hash_key in partitions:
            partition_id = int(partition_hash_key.split(":")[-1])

            logger.info(f"rewriting partition {partition_id}")
            span_store = RedisSubscriptionDataStore(
                redis_client,
                EntityKey.EAP_SPANS,
                PartitionId(partition_id),
            )
            item_store = RedisSubscriptionDataStore(
                redis_client,
                EntityKey.EAP_ITEMS,
                PartitionId(partition_id),
            )
            rewritten_count = 0
            for key, data in span_store.all():
                data.entity = EntityKey.EAP_ITEMS
                item_store.create(key, data)
                span_store.delete(key)
                rewritten_count += 1
            logger.info(
                f"sucessfully rewrote {rewritten_count} subscriptions for partition {partition_id}"
            )
