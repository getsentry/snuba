from datetime import datetime
from typing import Sequence, Set

from snuba.redis import RedisClientType

OPTIMIZE_PREFIX = "snuba-optimize"


class NoOptimizedStateException(Exception):
    """
    This exception indicates that there is no state stored in the optimized
    tracker.
    """

    pass


class OptimizedPartitionTracker:
    """
    This class keeps track of partitions which have already been
    optimized by keeping state of optimized partitions in redis.
    """

    def __init__(
        self,
        redis_client: RedisClientType,
        host: str,
        port: int,
        database: str,
        table: str,
        expire_time: datetime,
    ) -> None:
        self.__redis_client = redis_client
        self.__host = host
        self.__port = port
        self.__database = database
        self.__table = table
        today = datetime.now().date()
        common_prefix = (
            f"{OPTIMIZE_PREFIX}:{self.__host}:{self.__port}:{self.__database}:"
            f"{self.__table}:{today}"
        )
        self.__all_bucket = f"{common_prefix}:all"
        self.__completed_bucket = f"{common_prefix}:completed"
        self.__key_expire_time = expire_time

    def __get_partitions(self, bucket: str) -> Set[str]:
        """
        Get the partitions from a given bucket.
        """
        partitions_set: Set[str] = set()
        partitions = self.__redis_client.smembers(bucket)
        if partitions:
            for partition in partitions:
                assert isinstance(partition, bytes)
                partitions_set.add(partition.decode("utf-8"))

        return partitions_set

    def get_all_partitions(self) -> Set[str]:
        """
        Get a set of partitions which need to be optimized.
        """
        return self.__get_partitions(self.__all_bucket)

    def get_completed_partitions(self) -> Set[str]:
        """
        Get a set of partitions that have completed optimization.
        """
        return self.__get_partitions(self.__completed_bucket)

    def __update_partitions(self, bucket: str, encoded_part_names: Sequence[bytes]) -> None:
        """
        Update the partitions in the bucket.
        """
        with self.__redis_client.pipeline() as pipe:
            pipe.sadd(bucket, *encoded_part_names)
            pipe.expireat(bucket, self.__key_expire_time)
            pipe.execute()

    def update_all_partitions(self, part_names: Sequence[str]) -> None:
        """
        Update the list of all partitions which need to be optimized.
        """
        if len(part_names) == 0:
            return

        encoded_part_names = [part.encode("utf-8") for part in part_names]
        self.__update_partitions(self.__all_bucket, encoded_part_names)

    def update_completed_partitions(self, part_name: str) -> None:
        """
        Add partitions that have completed optimization.
        """
        self.__update_partitions(self.__completed_bucket, [part_name.encode("utf-8")])

    def get_partitions_to_optimize(self) -> Set[str]:
        """
        Get a set of partition names which need optimization.

        When getting the partitions to optimize, NoOptimizedStateException
        exception can be raised to indicate that there is no state
        information and we need to populate the state. The exception is
        returned when the optimization is run for the first time on a given
        day. In all other cases a set object is returned.

        """
        all_partitions = self.get_all_partitions()
        completed_partitions = self.get_completed_partitions()

        if not all_partitions:
            raise NoOptimizedStateException

        if not completed_partitions:
            return all_partitions
        else:
            return all_partitions - completed_partitions

    def delete_all_states(self) -> None:
        """
        Delete the sets of partitions which had to be optimized and
        which have already been optimized.
        """
        with self.__redis_client.pipeline() as pipe:
            pipe.delete(self.__all_bucket)
            pipe.delete(self.__completed_bucket)
            pipe.execute()
