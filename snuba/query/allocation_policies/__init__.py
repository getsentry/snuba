from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Any, cast

from snuba import settings
from snuba.clusters.storage_sets import StorageSetKey
from snuba.state import get_config
from snuba.utils.registered_class import RegisteredClass, import_submodules_in_directory
from snuba.utils.serializable_exception import JsonSerializable, SerializableException
from snuba.web import QueryException, QueryResult

logger = logging.getLogger("snuba.query.allocation_policy_base")
CAPMAN_PREFIX = "capman"


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

    @property
    def quota_allowance(self) -> dict[str, JsonSerializable]:
        return cast(
            "dict[str, JsonSerializable]", self.extra_data.get("quota_allowance", {})
        )

    @property
    def explanation(self) -> dict[str, JsonSerializable]:
        return self.extra_data.get("quota_allowance", {}).get("explanation", {})  # type: ignore

    def __str__(self) -> str:
        return f"{self.message}, explanation: {self.explanation}"


class AllocationPolicy(ABC, metaclass=RegisteredClass):
    """This class should be the centralized place for policy decisions regarding
    resource usage of a clickhouse cluster. It is meant to live as a configurable item
    on a storage.

    Examples of policy decisions include:

    * An organization_id may only scan this many bytes in an hour
    * A a referrer that has scanned this many bytes has this many max_threads to run their query

    To make your own allocation policy:

        >>> class MyAllocationPolicy(AllocationPolicy):

        >>>     def _get_quota_allowance(
        >>>         self, tenant_ids: dict[str, str | int]
        >>>     ) -> QuotaAllowance:
        >>>         # before a query is run on clickhouse, make a decision whether it can be run and with
        >>>         # how many threads
        >>>         pass

        >>>     def _update_quota_balance(
        >>>         self,
        >>>         tenant_ids: dict[str, str | int],
        >>>         result_or_error: QueryResultOrError,
        >>>     ) -> None:
        >>>         # after the query has been run, update whatever this allocation policy
        >>>         # keeps track of which will affect subsequent queries
        >>>         pass

    To use it:

        >>> policy = MyAllocationPolicy(
        >>>     StorageSetKey("mystorage"), required_tenant_types=["organization_id", "referrer"]
        >>> )
        >>> allowance = policy.get_quota_allowance({"organization_id": 1234, "referrer": "myreferrer"})
        >>> result = run_db_query(allowance)
        >>> policy.update_quota_balance(
        >>>     tenant_ids={"organization_id": 1234, "referrer": "myreferrer"},
        >>>     QueryResultOrError(result=result)
        >>> )

    The allocation policy base class has two public methods:
        * get_quota_allowance
        * update_quota_balance

    These functions can be used to modify the behaviour of ALL allocation policies, use with care.

    **GOTCHAS**
    -----------

    * Because allocation policies are attached to query objects, they have to be pickleable. Don't put non-pickleable members onto the allocation policy
    * At time of writing (29-03-2023), not all allocation policy decisions are made in the allocation policy,
        rate limiters are still applied in the query pipeline, those should be moved into an allocation policy as they
        are also policy decisions
    * get_quota_allowance will throw an AllocationPolicyViolation if _get_quota_allowance().can_run is false.
        this is to keep with the pattern in `db_query.py` which communicates error states with exceptions. There is no other
        reason. For more information see snuba.web.db_query.db_query
    * Every allocation policy takes a `storage_set_key` in its init. The storage_set_key is like a pseudo-tenant. In different
        environments, storages may be co-located on the same cluster. To facilitate resource sharing, every allocation policy
        knows which storage_set_key it is serving. This is currently not used
    """

    def __init__(
        self,
        storage_set_key: StorageSetKey,
        required_tenant_types: list[str],
        **kwargs: str,
    ) -> None:
        self._required_tenant_types = set(required_tenant_types)
        self._storage_set_key = storage_set_key

    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    @classmethod
    def get_from_name(cls, name: str) -> "AllocationPolicy":
        return cast("AllocationPolicy", cls.class_from_name(name))

    def get_current_configs(self) -> list[dict[str, Any]]:
        """Placeholder - doesn't actually do anything."""
        return [
            {
                "key": "some key",
                "value": "some value",
                "description": "Placeholder config. Will not actually be saved.",
                "type": "placeholder",
                "params": {},
            },
            {
                "key": "some_optional_key",
                "value": "some other value",
                "description": "Placeholder config. Will not actually be saved.",
                "type": "placeholder",
                "params": {"c": 3, "d": 4},
            },
        ]

    def get_optional_config_definitions(self) -> list[dict[str, Any]]:
        """
        Placeholder - doesn't actually do anything.
        This should return a list of configs that can be "added" to this policy.
        The only configs falling under this def should be configs that have params.
        """

        return [
            {
                "name": "some_optional_key",
                "type": "int",
                "default": 10,
                "description": "Placeholder config. Will not actually be saved.",
                "params": [{"name": "c", "type": "int"}, {"name": "d", "type": "int"}],
            }
        ]

    def set_config(
        self, config_key: str, value: Any, user: str | None, params: dict[str, Any] = {}
    ) -> dict[str, Any]:
        """Placeholder - doesn't actually do anything."""
        return {
            "key": config_key,
            "value": value,
            "description": "Placeholder config. Will not actually be saved.",
            "type": "placeholder",
            "params": params,
        }

    def delete_config(
        self, config_key: str, user: str | None, params: dict[str, Any] = {}
    ) -> None:
        """Placeholder - doesn't actually do anything."""
        pass

    def __eq__(self, other: Any) -> bool:
        """There should not be a need to compare these except that
        AllocationPolicies are attached to the Table a query is executed against.
        In order to allow that comparison, this function is implemented here.
        """
        return (
            bool(self.__class__ == other.__class__)
            and self._storage_set_key == other._storage_set_key
            and self._required_tenant_types == other._required_tenant_types
        )

    @classmethod
    def from_kwargs(cls, **kwargs: str) -> "AllocationPolicy":
        required_tenant_types = kwargs.pop("required_tenant_types", None)
        storage_set_key = kwargs.pop("storage_set_key", None)
        assert isinstance(
            required_tenant_types, list
        ), "required_tenant_types must be a list of strings"
        assert isinstance(storage_set_key, str)
        return cls(
            required_tenant_types=required_tenant_types,
            storage_set_key=StorageSetKey(storage_set_key),
            **kwargs,
        )

    def get_quota_allowance(self, tenant_ids: dict[str, str | int]) -> QuotaAllowance:
        try:
            allowance = self._get_quota_allowance(tenant_ids)
        except Exception:
            logger.exception(
                "Allocation policy failed to get quota allowance, this is a bug, fix it"
            )
            if settings.RAISE_ON_ALLOCATION_POLICY_FAILURES:
                raise
            return DEFAULT_PASSTHROUGH_POLICY.get_quota_allowance(tenant_ids)
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
        try:
            return self._update_quota_balance(tenant_ids, result_or_error)
        except Exception:
            logger.exception(
                "Allocation policy failed to update quota balance, this is a bug, fix it"
            )
            if settings.RAISE_ON_ALLOCATION_POLICY_FAILURES:
                raise

    @abstractmethod
    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        result_or_error: QueryResultOrError,
    ) -> None:
        pass


class PassthroughPolicy(AllocationPolicy):
    def _get_quota_allowance(self, tenant_ids: dict[str, str | int]) -> QuotaAllowance:

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
    StorageSetKey("default.no_storage_set_key"), required_tenant_types=[]
)

import_submodules_in_directory(
    os.path.dirname(os.path.realpath(__file__)), "snuba.query.allocation_policies"
)
