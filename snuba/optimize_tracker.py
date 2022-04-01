from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Set

from snuba.redis import RedisClientType

OPTIMIZE_PREFIX = "snuba-optimize"


class OptimizedPartitionTracker(ABC):
    """
    OptimizedPartitionTracker is an abstract class for tracking progress of an optimize job.
    """

    @abstractmethod
    def get_completed_partitions(self) -> Optional[Set[str]]:
        """
        Get a list of partitions that have completed optimization.
        """
        raise NotImplementedError

    @abstractmethod
    def update_completed_partitions(self, part_name: str) -> None:
        """
        Add partitions that have completed optimization.
        """
        raise NotImplementedError

    @abstractmethod
    def remove_all_partitions(self) -> None:
        """
        Remove all partitions that have completed optimization.
        """
        raise NotImplementedError


class RedisOptimizedPartitionTracker(OptimizedPartitionTracker):
    """
    This class keeps track of partitions which have already been optimized by keeping state of
    optimized partitions in redis.
    """

    def __init__(
        self, redis_client: RedisClientType, host: str, expire_time: datetime
    ) -> None:
        self.__redis_client = redis_client
        self.__bucket = f"{OPTIMIZE_PREFIX}:{host}"
        self.__key_expire_time = expire_time

    def get_completed_partitions(self) -> Optional[Set[str]]:
        completed_partitions = self.__redis_client.smembers(self.__bucket)
        if not completed_partitions:
            return None

        partitions_set: Set[str] = set()
        for partition in completed_partitions:
            assert isinstance(partition, bytes)
            partitions_set.add(partition.decode("utf-8"))

        return partitions_set

    def update_completed_partitions(self, part_name: str) -> None:
        pipe = self.__redis_client.pipeline()
        pipe.sadd(self.__bucket, part_name.encode("utf-8"))
        pipe.expireat(self.__bucket, self.__key_expire_time)
        pipe.execute()

    def remove_all_partitions(self) -> None:
        self.__redis_client.delete(self.__bucket)
