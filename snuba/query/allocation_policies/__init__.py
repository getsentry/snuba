from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Any, cast

from snuba.clusters.storage_sets import StorageSetKey
from snuba.utils.registered_class import RegisteredClass
from snuba.utils.serializable_exception import SerializableException
from snuba.web import QueryException, QueryResult


@dataclass(frozen=True)
class QueryResultOrError:
    """When a query executes, even if it errors, we still want the stats associated
    with the query and what the error was (as the type of error may be penalized
    differently"""

    query_result: QueryResult | None
    error: QueryException | None

    def __post_init__(self) -> None:
        assert self.query_result is not None or self.error is not None


@dataclass(frozen=True)
class QuotaAllowance:
    can_run: bool
    max_threads: int
    # if any limiting action was taken by the allocation
    # policy, this dictionary should contain some information
    # about what caused that action. Not currently well typed
    # because I don't know what exactly should go in it yet
    explanation: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class AllocationPolicyViolation(SerializableException):
    @classmethod
    def from_args(
        cls, tenant_ids: dict[str, str | int], quota_allowance: QuotaAllowance
    ) -> "AllocationPolicyViolation":
        return cls(
            "Allocation policy violated",
            tenant_ids=tenant_ids,
            quota_allowance=quota_allowance.to_dict(),
        )


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

    def __eq__(self, other: Any) -> bool:
        """There should not be a need to compare these except that
        AllocationPolicies are attached to the Table a query is executed against.
        In order to allow that comparison, this function is implemented here.
        """
        return bool(self.__class__ == other.__class__)

    @classmethod
    def from_kwargs(cls, **kwargs: str) -> "AllocationPolicy":
        accepted_tenant_types = kwargs.pop("accepted_tenant_types", None)
        storage_set_key = kwargs.pop("storage_set_key", None)
        assert isinstance(
            accepted_tenant_types, list
        ), "accepted_tenant_types must be a list of strings"
        assert isinstance(storage_set_key, str)
        return cls(
            accepted_tenant_types=accepted_tenant_types,
            storage_set_key=StorageSetKey(storage_set_key),
            **kwargs,
        )

    def get_quota_allowance(self, tenant_ids: dict[str, str | int]) -> QuotaAllowance:
        allowance = self._get_quota_allowance(tenant_ids)
        if not allowance.can_run:
            raise AllocationPolicyViolation.from_args(tenant_ids, allowance)
        return allowance

    @abstractmethod
    def _get_quota_allowance(self, tenant_ids: dict[str, str | int]) -> QuotaAllowance:
        pass

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

        max_threads = get_config("query_settings/max_threads", 8)
        assert isinstance(max_threads, int)
        return QuotaAllowance(can_run=True, max_threads=max_threads, explanation={})

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        result_or_error: QueryResultOrError,
    ) -> None:
        pass


DEFAULT_PASSTHROUGH_POLICY = PassthroughPolicy(
    StorageSetKey("default.no_storage_set_key"), accepted_tenant_types=[]
)
