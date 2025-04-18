from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.manual_jobs import Job, JobLogger, JobSpec
from snuba.redis import RedisClientKey, get_redis_client
from snuba.subscriptions.data import PartitionId, RPCSubscriptionData
from snuba.subscriptions.store import RedisSubscriptionDataStore

redis_client = get_redis_client(RedisClientKey.SUBSCRIPTION_STORE)


class RewriteSubscriptionEntity(Job):
    """
    A manual job to rewrite the entity field in subscriptions table.
    """

    def __init__(self, job_spec: JobSpec) -> None:
        super().__init__(job_spec)
        assert job_spec.params is not None
        assert "source_entity" in job_spec.params
        assert "target_entity" in job_spec.params
        assert isinstance(job_spec.params["source_entity"], str)
        assert isinstance(job_spec.params["target_entity"], str)
        self.__source_entity_key = EntityKey(job_spec.params["source_entity"])
        self.__target_entity_key = EntityKey(job_spec.params["target_entity"])
        self.__target_entity = get_entity(self.__target_entity_key)

    def execute(self, logger: JobLogger) -> None:
        """
        Executes the job to migrate subscription entities, without
        changing the underlying subscription data (as much as possible). This
        does require the subscription data to be RPC-encoded (as in EAP).
        """
        partitions = redis_client.keys(
            f"subscriptions:{self.__source_entity_key.value}:*"
        )
        for partition_hash_key in partitions:
            partition_id = int(partition_hash_key.decode("utf-8").split(":")[-1])

            logger.info(
                f"rewriting partition {partition_id} from {self.__source_entity_key} to {self.__target_entity_key}"
            )
            span_store = RedisSubscriptionDataStore(
                redis_client,
                self.__source_entity_key,
                PartitionId(partition_id),
            )
            item_store = RedisSubscriptionDataStore(
                redis_client,
                self.__target_entity_key,
                PartitionId(partition_id),
            )

            rewritten_count = 0
            for key, data in span_store.all():
                assert isinstance(data, RPCSubscriptionData)
                copied_subscription_entity = RPCSubscriptionData(
                    project_id=data.project_id,
                    resolution_sec=data.resolution_sec,
                    time_window_sec=data.time_window_sec,
                    entity=self.__target_entity,
                    metadata=data.metadata,
                    time_series_request=data.time_series_request,
                    request_name=data.request_name,
                    request_version=data.request_version,
                )
                item_store.create(key, copied_subscription_entity)
                span_store.delete(key)
                rewritten_count += 1

            logger.info(
                f"sucessfully rewrote {rewritten_count} subscriptions for partition {partition_id}"
            )
