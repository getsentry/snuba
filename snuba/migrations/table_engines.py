from abc import ABC, abstractmethod

from typing import Mapping, Optional


class TableEngine(ABC):
    """
    Represents a ClickHouse table engine, such as any table or view.

    Any changes made to these classes should ensure the result of every migration
    does not change.
    """

    @abstractmethod
    def get_sql(self) -> str:
        raise NotImplementedError


class MergeTree(TableEngine):
    def __init__(
        self,
        order_by: str,
        partition_by: Optional[str] = None,
        sample_by: Optional[str] = None,
        ttl: Optional[str] = None,
        settings: Optional[Mapping[str, str]] = None,
    ) -> None:
        self.__order_by = order_by
        self.__partition_by = partition_by
        self.__sample_by = sample_by
        self.__ttl = ttl
        self.__settings = settings

    def get_sql(self) -> str:
        sql = f"{self._get_engine_type()} ORDER BY {self.__order_by}"

        if self.__partition_by:
            sql += f" PARTITION BY {self.__partition_by}"

        if self.__sample_by:
            sql += f" SAMPLE BY {self.__sample_by}"

        if self.__ttl:
            sql += f" TTL {self.__ttl}"

        if self.__settings:
            settings = ", ".join([f"{k}={v}" for k, v in self.__settings.items()])
            sql += f" SETTINGS {settings}"

        return sql

    def _get_engine_type(self) -> str:
        return "MergeTree()"


class ReplacingMergeTree(MergeTree):
    def __init__(
        self,
        version_column: str,
        order_by: str,
        partition_by: Optional[str] = None,
        sample_by: Optional[str] = None,
        ttl: Optional[str] = None,
        settings: Optional[Mapping[str, str]] = None,
    ) -> None:
        super().__init__(order_by, partition_by, sample_by, ttl, settings)
        self.__version_column = version_column

    def _get_engine_type(self) -> str:
        return f"ReplacingMergeTree({self.__version_column})"
