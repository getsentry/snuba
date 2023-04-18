from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Any, cast

from snuba import settings
from snuba.datasets.storages.storage_key import StorageKey
from snuba.state import get_config as get_runtime_config
from snuba.state import set_config as set_runtime_config
from snuba.utils.registered_class import RegisteredClass, import_submodules_in_directory
from snuba.utils.serializable_exception import JsonSerializable, SerializableException
from snuba.web import QueryException, QueryResult

logger = logging.getLogger("snuba.query.allocation_policy_base")

CAPMAN_PREFIX = "capman"
IS_ACTIVE = "is_active"
IS_ENFORCED = "is_enforced"


@dataclass(frozen=True)
class QueryResultOrError:
    """When a query executes, even if it errors, we still want the stats associated
    with the query and what the error was (as the type of error may be penalized
    differently"""

    query_result: QueryResult | None
    error: QueryException | None

    def __post_init__(self) -> None:
        assert self.query_result is not None or self.error is not None


@dataclass()
class AllocationPolicyConfig:
    name: str
    description: str
    type: type
    default: Any
    current_value: Any | None = None


class InvalidPolicyConfig(Exception):
    pass


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
        storage_key: StorageKey,
        required_tenant_types: list[str],
        **kwargs: str,
    ) -> None:
        self._required_tenant_types = set(required_tenant_types)
        self._storage_key = storage_key
        self._default_config_params = [
            AllocationPolicyConfig(
                name=IS_ACTIVE,
                description="Whether or not this policy is active.",
                type=int,
                default=1,
            ),
            AllocationPolicyConfig(
                name=IS_ENFORCED,
                description="Whether or not this policy is enforced.",
                type=int,
                default=0,
            ),
        ]

    @property
    def runtime_config_prefix(self) -> str:
        return f"{CAPMAN_PREFIX}.{self._storage_key.value}.{self.__class__.__name__}"

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
        return (
            bool(self.__class__ == other.__class__)
            and self._storage_key == other._storage_key
            and self._required_tenant_types == other._required_tenant_types
        )

    @classmethod
    def from_kwargs(cls, **kwargs: str) -> "AllocationPolicy":
        required_tenant_types = kwargs.pop("required_tenant_types", None)
        storage_key = kwargs.pop("storage_key", None)
        assert isinstance(
            required_tenant_types, list
        ), "required_tenant_types must be a list of strings"
        assert isinstance(storage_key, str)
        return cls(
            required_tenant_types=required_tenant_types,
            storage_key=StorageKey(storage_key),
            **kwargs,
        )

    @abstractmethod
    def _config_params(self) -> list[AllocationPolicyConfig]:
        """
        Define policy specific config params, these will be used along
        with the default params of the base class.
        """
        pass

    def __raw_config_params(self) -> dict[str, AllocationPolicyConfig]:
        """
        Returns a dictionary of configurable params on this AllocationPolicy.
        The `current_value` is not guaranteed to be up to date on the config object.
        """
        return {
            config.name: config
            for config in self._default_config_params + self._config_params()
        }

    def config_params(self) -> dict[str, AllocationPolicyConfig]:
        """
        Returns a list of the configurable params of this AllocationPolicy
        with up to date current values of the params.
        """
        configurable_params = self.__raw_config_params()
        for param in configurable_params:
            configurable_params[param].current_value = self.get_config(param)
        return configurable_params

    def __build_runtime_config_key(self, config: str) -> str:
        return f"{self.runtime_config_prefix}.{config}"

    def get_config(self, config: str) -> Any:
        """Returns current value of a config on this Allocation Policy, or the default if none exists in Redis."""
        configurable_params = self.__raw_config_params()
        if config not in configurable_params:
            raise InvalidPolicyConfig(
                f"'{config}' is not a valid config for {self.__class__.__name__}!"
            )
        config_params = configurable_params[config]
        return cast(
            config_params.type,  # type: ignore
            get_runtime_config(
                self.__build_runtime_config_key(config), config_params.default
            ),
        )

    def set_config(self, config: str, value: Any) -> Any:
        """Sets a value of a config on this Allocation Policy."""
        configurable_params = self.__raw_config_params()
        if config not in configurable_params:
            raise InvalidPolicyConfig(
                f"'{config}' is not a valid config for {self.__class__.__name__}!"
            )
        expected_type = configurable_params[config].type
        if not isinstance(value, expected_type):
            raise InvalidPolicyConfig(
                f"'{value}' ({type(value).__name__}) is not of expected type: {expected_type.__name__}"
            )
        set_runtime_config(self.__build_runtime_config_key(config), value)

    def is_active(self) -> bool:
        return bool(self.get_config(IS_ACTIVE))

    def is_enforced(self) -> bool:
        return bool(self.get_config(IS_ENFORCED))

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
    def _config_params(self) -> list[AllocationPolicyConfig]:
        return []

    def _get_quota_allowance(self, tenant_ids: dict[str, str | int]) -> QuotaAllowance:

        max_threads = get_runtime_config("query_settings/max_threads", 8)
        assert isinstance(max_threads, int)
        return QuotaAllowance(can_run=True, max_threads=max_threads, explanation={})

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        result_or_error: QueryResultOrError,
    ) -> None:
        pass


DEFAULT_PASSTHROUGH_POLICY = PassthroughPolicy(
    StorageKey("default.no_storage_key"),
    required_tenant_types=[],
)

import_submodules_in_directory(
    os.path.dirname(os.path.realpath(__file__)), "snuba.query.allocation_policies"
)
