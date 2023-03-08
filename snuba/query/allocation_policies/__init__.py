from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import cast

from snuba.datasets.storages.storage_key import StorageKey
from snuba.state import get_config
from snuba.utils.registered_class import RegisteredClass
from snuba.web import QueryResult


@dataclass(frozen=True)
class QuotaAllowance:
    can_run: bool
    max_threads: int
    # TODO: tenants over quota could be more descriptive
    tenants_over_quota: list[str]


class AllocationPolicy(ABC, metaclass=RegisteredClass):
    def __init__(self, accepted_tenant_types: list[str], **kwargs) -> None:
        self._accepted_tenant_types = set(accepted_tenant_types)

    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    @classmethod
    def get_from_name(cls, name: str) -> "AllocationPolicy":
        return cast("AllocationPolicy", cls.class_from_name(name))

    @classmethod
    def from_kwargs(cls, **kwargs: str) -> "AllocationPolicy":
        accepted_tenant_types = kwargs.pop("accepted_tenant_types", None)
        assert isinstance(
            accepted_tenant_types, list
        ), "accepted_tenant_types must be a list of strings"
        return cls(accepted_tenant_types=accepted_tenant_types, **kwargs)

    def get_quota_allowance(
        self, tenant_ids: dict[str, str | int], storage_key: StorageKey
    ) -> QuotaAllowance:
        return self._get_quota_allowance(tenant_ids, storage_key)

    @abstractmethod
    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], storage_key: StorageKey
    ) -> QuotaAllowance:
        pass

        return self._get_quota_allowance(tenant_ids, storage_key)

    def update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        storage_key: StorageKey,
        query_result: QueryResult,
    ) -> None:
        return self._update_quota_balance(tenant_ids, storage_key, query_result)

    @abstractmethod
    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        storage_key: StorageKey,
        query_result: QueryResult,
    ) -> None:
        pass


class PassthroughPolicy(AllocationPolicy):
    def __init__(self, accepted_tenant_types: list[str]):
        pass

    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], storage_key: StorageKey
    ) -> QuotaAllowance:
        max_threads = get_config("query_settings/max_threads", 10)
        assert isinstance(max_threads, int)
        return QuotaAllowance(
            can_run=True, max_threads=max_threads, tenants_over_quota=[]
        )

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        storage_key: StorageKey,
        query_result: QueryResult,
    ) -> None:
        pass
