from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Set

from snuba import state
from snuba.state import rds


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


class InMemoryPartitionTracker(OptimizedPartitionTracker):
    """
    Tracks progress of partitions that have completed optimization using in memory data storage.
    """

    def __init__(self) -> None:
        self.__tracker: Set[str] = set()

    def get_completed_partitions(self) -> Optional[Set[str]]:
        return self.__tracker if len(self.__tracker) > 0 else None

    def update_completed_partitions(self, part_name: str) -> None:
        self.__tracker.add(part_name)

    def remove_all_partitions(self) -> None:
        self.__tracker.clear()


class RedisOptimizedPartitionTracker(OptimizedPartitionTracker):
    """
    This class keeps track of partitions which have already been optimized by keeping state of
    optimized partitions in redis.
    """

    def __init__(self, host: str, expire_time: datetime) -> None:
        self.__bucket = f"{state.optimize_prefix}{host}"
        self.__key_expire_time = expire_time

    def get_completed_partitions(self) -> Optional[Set[str]]:
        completed_partitions = rds.smembers(self.__bucket)
        if not completed_partitions:
            return None

        partitions_set: Set[str] = set()
        for partition in completed_partitions:
            assert isinstance(partition, bytes)
            partitions_set.add(partition.decode("utf-8"))

        return partitions_set

    def update_completed_partitions(self, part_name: str) -> None:
        rds.sadd(self.__bucket, part_name.encode("utf-8"))
        rds.expireat(self.__bucket, self.__key_expire_time)

    def remove_all_partitions(self) -> None:
        rds.delete(self.__bucket)
