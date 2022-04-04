from datetime import datetime
from typing import Any, Optional, Sequence, Set

from snuba.redis import RedisClientType

OPTIMIZE_PREFIX = "snuba-optimize"


class OptimizedPartitionTracker:
    """
    This class keeps track of partitions which have already been
    optimized by keeping state of optimized partitions in redis.
    """

    def __init__(
        self,
        redis_client: RedisClientType,
        host: str,
        database: str,
        table: str,
        expire_time: datetime,
    ) -> None:
        self.__redis_client = redis_client
        self.__host = host
        self.__database = database
        self.__table = table
        today = datetime.now().date()
        common_prefix = (
            f"{OPTIMIZE_PREFIX}:{self.__host}:{self.__database}:"
            f"{self.__table}:{today}"
        )
        self.__all_bucket = f"{common_prefix}:all"
        self.__completed_bucket = f"{common_prefix}:completed"
        self.__key_expire_time = expire_time

    @staticmethod
    def __redis_set_of_bytes_to_set_of_strings(values: Any) -> Set[str]:
        partitions_set: Set[str] = set()
        for partition in values:
            assert isinstance(partition, bytes)
            partitions_set.add(partition.decode("utf-8"))

        return partitions_set

    def get_all_partitions(self) -> Optional[Set[str]]:
        """
        Get a set of all partitions which need to be optimized.
        """
        all_partitions = self.__redis_client.smembers(self.__all_bucket)
        if not all_partitions:
            return None

        return self.__redis_set_of_bytes_to_set_of_strings(all_partitions)

    def update_all_partitions(self, part_names: Sequence[str]) -> None:
        """
        Update the list of all partitions which need to be optimized.
        """
        encoded_part_names = [part.encode("utf-8") for part in part_names]
        pipe = self.__redis_client.pipeline()
        pipe.sadd(self.__all_bucket, *encoded_part_names)
        pipe.expireat(self.__all_bucket, self.__key_expire_time)
        pipe.execute()

    def get_completed_partitions(self) -> Optional[Set[str]]:
        """
        Get a set of partitions that have completed optimization.
        """
        completed_partitions = self.__redis_client.smembers(self.__completed_bucket)
        if not completed_partitions:
            return None

        return self.__redis_set_of_bytes_to_set_of_strings(completed_partitions)

    def update_completed_partitions(self, part_name: str) -> None:
        """
        Add partitions that have completed optimization.
        """
        pipe = self.__redis_client.pipeline()
        pipe.sadd(self.__completed_bucket, part_name.encode("utf-8"))
        pipe.expireat(self.__completed_bucket, self.__key_expire_time)
        pipe.execute()

    def remove_all_partitions(self) -> None:
        """
        Delete the sets of partitions which had to be optimized and
        which have already been optimized.
        """
        pipe = self.__redis_client.pipeline()
        pipe.delete(self.__all_bucket)
        pipe.delete(self.__completed_bucket)
        pipe.execute()
