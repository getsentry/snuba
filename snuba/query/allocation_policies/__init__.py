from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import partial
from typing import Callable, cast

from snuba.clusters.storage_sets import StorageSetKey
from snuba.utils.registered_class import RegisteredClass
from snuba.web import QueryException, QueryResult


@dataclass(frozen=True)
class QueryResultOrError:
    """When a query executes, even if it errors, we still want the stats associated
    with the query and what the error was (as the type of error may be penalized
    differently"""

    query_result: QueryResult | None
    error: QueryException | None

    def __post_init__(self):
        assert self.query_result is not None or self.error is not None


@dataclass(frozen=True)
class QuotaAllowance:
    can_run: bool
    max_threads: int
    # TODO: tenants over quota could be more descriptive
    tenants_over_quota: list[str]


class AllocationPolicy(ABC, metaclass=RegisteredClass):
    def __init__(
        self,
        storage_set_key: StorageSetKey,
        accepted_tenant_types: list[str],
        **kwargs: str,
    ) -> None:
        self._accepted_tenant_types = set(accepted_tenant_types)

    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    @classmethod
    def get_from_name(cls, name: str) -> "AllocationPolicy":
        return cast("AllocationPolicy", cls.class_from_name(name))

    @classmethod
    def from_kwargs(
        cls, **kwargs: str
    ) -> Callable[[StorageSetKey], "AllocationPolicy"]:
        accepted_tenant_types = kwargs.pop("accepted_tenant_types", None)
        assert isinstance(
            accepted_tenant_types, list
        ), "accepted_tenant_types must be a list of strings"
        return partial(cls, accepted_tenant_types=accepted_tenant_types, **kwargs)

    def get_quota_allowance(self, tenant_ids: dict[str, str | int]) -> QuotaAllowance:
        return self._get_quota_allowance(tenant_ids)

    @abstractmethod
    def _get_quota_allowance(self, tenant_ids: dict[str, str | int]) -> QuotaAllowance:
        pass

        return self._get_quota_allowance(tenant_ids)

    def update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        result_or_error: QueryResultOrError,
    ) -> None:
        return self._update_quota_balance(tenant_ids, result_or_error)

    @abstractmethod
    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        result_or_error: QueryResultOrError,
    ) -> None:
        pass


class PassthroughPolicy(AllocationPolicy):
    def __init__(
        self, storage_set_key: StorageSetKey, accepted_tenant_types: list[str]
    ) -> None:
        pass

    def _get_quota_allowance(self, tenant_ids: dict[str, str | int]) -> QuotaAllowance:
        from snuba.state import get_config

        max_threads = get_config("query_settings/max_threads", 10)
        assert isinstance(max_threads, int)
        return QuotaAllowance(
            can_run=True, max_threads=max_threads, tenants_over_quota=[]
        )

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        result_or_error: QueryResultOrError,
    ) -> None:
        pass
