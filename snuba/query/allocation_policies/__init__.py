from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Type, cast

from snuba import environment, settings
from snuba.configs.configuration import (
    ConfigurableComponent,
    ConfigurableComponentData,
    Configuration,
    ResourceIdentifier,
    logger,
)
from snuba.datasets.storages.storage_key import StorageKey
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.registered_class import import_submodules_in_directory
from snuba.utils.serializable_exception import JsonSerializable, SerializableException
from snuba.web import QueryException, QueryResult

CAPMAN_PREFIX = "capman"
CAPMAN_HASH = "capman"

IS_ACTIVE = "is_active"
IS_ENFORCED = "is_enforced"
MAX_THREADS = "max_threads"
NO_UNITS = "no_units"
NO_SUGGESTION = "no_suggestion"
CROSS_ORG_SUGGESTION = "cross org queries do not have limits"
PASS_THROUGH_REFERRERS_SUGGESTION = (
    "subscriptions currently do not undergo rate limiting in any way"
)
MAX_THRESHOLD = int(1e12)


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
class AllocationPolicyConfig(Configuration):
    pass


@dataclass(frozen=True)
class QuotaAllowance:
    can_run: bool
    max_threads: int
    # if any limiting action was taken by the allocation
    # policy, this dictionary should contain some information
    # about what caused that action. Not currently well typed
    # because I don't know what exactly should go in it yet
    explanation: dict[str, JsonSerializable]
    is_throttled: bool
    throttle_threshold: int
    rejection_threshold: int
    quota_used: int
    quota_unit: str
    suggestion: str

    # sets this value:
    # https://clickhouse.com/docs/operations/settings/settings#max_bytes_to_read
    # 0 means unlimited
    max_bytes_to_read: int = field(default=0)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, QuotaAllowance):
            return False
        return (
            self.can_run == other.can_run
            and self.max_threads == other.max_threads
            and self.max_bytes_to_read == other.max_bytes_to_read
            and self.explanation == other.explanation
            and self.is_throttled == other.is_throttled
            and self.throttle_threshold == other.throttle_threshold
            and self.rejection_threshold == other.rejection_threshold
            and self.quota_used == other.quota_used
            and self.quota_unit == other.quota_unit
            and self.suggestion == other.suggestion
        )


class InvalidTenantsForAllocationPolicy(SerializableException):
    """Individual policies can raise this exception if they are given invalid tenant_ids."""

    @classmethod
    def from_args(
        cls,
        tenant_ids: dict[str, str | int],
        policy_name: str,
        description: str | None = None,
    ) -> "InvalidTenantsForAllocationPolicy":
        return cls(
            description or "Invalid tenants for allocation policy",
            tenant_ids=tenant_ids,
            policy_name=policy_name,
        )


class AllocationPolicyViolations(SerializableException):
    """
    An exception class which is used to communicate that the query cannot be run because
    at least one policy of many said no
    """

    def __str__(self) -> str:
        return f"{self.message}, info: {{'details': {self.violations}, 'summary': {self.summary}}}"

    @property
    def violations(self) -> dict[str, dict[str, Any]]:
        details = cast(dict[str, Any], self.quota_allowance.get("details"))
        return {k: v for k, v in details.items() if v["can_run"] == False}

    @property
    def quota_allowance(self) -> dict[str, dict[str, Any]]:
        return cast(dict[str, dict[str, Any]], self.extra_data.get("quota_allowances", {}))

    @property
    def summary(self) -> dict[str, Any]:
        return self.quota_allowance.get("summary", {})

    @classmethod
    def from_args(
        cls,
        quota_allowances: dict[str, Any],
    ) -> "AllocationPolicyViolations":
        return cls(
            "Query on could not be run due to allocation policies",
            quota_allowances=quota_allowances,
        )


class PolicyData(ConfigurableComponentData):
    query_type: str


class QueryType(Enum):
    SELECT = "select"
    DELETE = "delete"


class AllocationPolicy(ConfigurableComponent, ABC):
    """This class should be the centralized place for policy decisions regarding
    resource usage of a clickhouse cluster. It is meant to live as a configurable item
    on a storage.

    Examples of policy decisions include:

    * An organization_id may only scan this many bytes in an hour
    * A a referrer that has scanned this many bytes has this many max_threads to run their query

    To make your own allocation policy:
    ===================================

        >>> class MyAllocationPolicy(AllocationPolicy):

        >>>    def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
        >>>         # Define policy specific config definitions, these will be used along
        >>>         # with the default definitions of the base class. (is_enforced, is_active)
        >>>         pass

        >>>     # Use your configs in the following methods

        >>>     def _get_quota_allowance(
        >>>         self, tenant_ids: dict[str, str | int], query_id: str
        >>>     ) -> QuotaAllowance:
        >>>         # before a query is run on clickhouse, make a decision whether it can be run and with
        >>>         # how many threads
        >>>         pass

        >>>     def _update_quota_balance(
        >>>         self,
        >>>         tenant_ids: dict[str, str | int],
        >>>         query_id: str
        >>>         result_or_error: QueryResultOrError,
        >>>     ) -> None:
        >>>         # after the query has been run, update whatever this allocation policy
        >>>         # keeps track of which will affect subsequent queries
        >>>         pass

    To use it:

        >>> policy = MyAllocationPolicy(
        >>>     StorageKey("mystorage"), required_tenant_types=["organization_id", "referrer"]
        >>> )
        >>> allowance = policy.get_quota_allowance({"organization_id": 1234, "referrer": "myreferrer"}, query_id="deadbeef")
        >>> result = run_db_query(allowance)
        >>> policy.update_quota_balance(
        >>>     tenant_ids={"organization_id": 1234, "referrer": "myreferrer"},
        >>>     query_id="deadbeef",
        >>>     QueryResultOrError(result=result)
        >>> )

    The allocation policy base class has two public methods for working with the actual quota:
        * get_quota_allowance
        * update_quota_balance

    These functions can be used to modify the behaviour of ALL allocation policies, use with care.

    Configurations
    ==============

    AllocationPolicy Configurations are a way to update live flags without shipping code changes. Very similar to runtime config
    (uses it under the hood) but with key differences and restrictions on what they can be.

    Any configuration definition that exists in your sub class' `_additional_config_definitions()` will appear in the
    Capacity Management Snuba Admin UI for the policy. From there you can modify the live values to alter how your policy works.

    The base class comes with 2 built in configs which are accessible as properties of the class itself:
    - is_active
        - Use this as a way to skip the policy entirely
    - is_enforced
        - Use this to throttle/reject queries OR just log stuff (assuming policy is active)

    Eg.

    >>> if not self.is_active:
    >>>     return
    >>> metrics.increment("something")
    >>> if self.is_enforced:
    >>>     # throttle query

    How to add additional configurations
    Additional configurations:
    - Required:
        - These are configurations you can add that have no parameters, this could be some amount you want to throttle
          queries to according to some logic you've written
    - Optional
        - These are configs you add that DO have parameters. These are meant to be duplicated as many times as you'd like
          with different parameters.
        - Eg. Some config that limits queries for a specific organization.
        - These are also the only configs that will show up when you go to "add new" config in the admin UI.

    Examples:

    Required Config: Some sort of queries per second rate limiter using a required config named "qps_limit"

        >>> def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
        >>>     return [
        >>>         AllocationPolicyConfig(
        >>>             name="qps_limit",
        >>>             description="Maximum of queries we can run per second",
        >>>             value_type=int,
        >>>             default=10,
        >>>         ),
        >>>         ...
        >>>     ]
        >>>
        >>> def _get_quota_allowance(...) -> QuotaAllowance:
        >>>     if self.__get_current_qps() < self.get_config_value("qps_limit"):
        >>>         return QuotaAllowance(can_run=True, ...)
        >>>     return QuotaAllowance(can_run=False, ...)
        >>>
        >>> def _update_quota_balance(...) -> None:
        >>>     self.__add_query_hit()

    Now this "qps_limit" config will show up in the Capacity Management UI and will be modifiable to any new integer and
    resettable to it's default. It cannot be deleted using the UI and must be removed from the actual code to go away.

    Optional Config: Same example but let's say certain referrers shouldn't count towards the QPS limit count

        >>> def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
        >>>     return [
        >>>         AllocationPolicyConfig(
        >>>             name="qps_limit_referrer_override",
        >>>             description="Referrer based override for the qps_limit config",
        >>>             value_type=int,
        >>>             default=0,
        >>>             param_types={"referrer": str},  # giving a value to `param_types` is what defines this as config optional
        >>>         ),
        >>>         ...
        >>>     ]
        >>>
        >>> def _get_quota_allowance(...) -> QuotaAllowance:
        >>>      # same as before
        >>>     ...
        >>>
        >>> def _update_quota_balance(...) -> None:
        >>>     # don't count this query towards the quota if the referrer is overridden
        >>>     if self.get_config_value("qps_limit_referrer_override", params={"referrer": tenant_ids.get("referrer")}):
        >>>         return
        >>>     self.__add_query_hit()

    Now this "qps_limit_referrer_override" config won't show up as an existing config in the UI, but when you go to
    "add new" config, it will be part of the list of optional configs you can add. From there you can create an instance
    of this config with the value 1 for a certain referrer and it'll show up in the configs list.

    Overriding Policy Default Configurations
    ----------------------------------------
    Every AllocationPolicyConfig comes with a default value specified in code. That can be overridden for any specific instance of the policy
    Example:

        >>> policy = MyAllocationPolicy(
        >>>     storage_key=StorageKey("some_storage"),
        >>>     required_tenant_types=["foo"],
        >>>     # This dictionary overrides whatever defaults are set
        >>>     # for this class
        >>>     default_config_overrides={
        >>>         "is_enforced": False, "fart_noise_level": 100
        >>>     }
        >>> )


    NOTE:
    - You should no longer use `snuba.state.{get,set}_config()` for runtime configs for a specific Policy. Use the
        `self.{get,set}_config_value()` methods on the policy itself instead! The only exception here is if you need to access or
        set some sort of global runtime config which would show up in the runtime config UI instead of Capacity Management.
    - If for some reason you find yourself needing to use a global config from snuba.state, consider adding it as a property to
        this base class since it should be universally useful across policies. An example of this is the `max_threads` property.


    **GOTCHAS**
    -----------

    * Because allocation policies are attached to query objects, they have to be pickleable. Don't put non-pickleable members onto the allocation policy
    * At time of writing (29-03-2023), not all allocation policy decisions are made in the allocation policy,
        table rate limiters are still applied in the query pipeline, those should be moved into an allocation policy as they
        are also policy decisions
    * Every allocation policy takes a `storage_key` in its init. The storage_key is like a pseudo-tenant. In different
        environments, storages may be co-located on the same cluster. To facilitate resource sharing, every allocation policy
        knows which storage_key it is serving. This is used to create unique keys for saving the config values.
        See `__build_runtime_config_key()` for more info.
    * Reiterating that you should no longer use `snuba.state.{get,set}_config()` for runtime configs for a specific Policy. Refer to the Configurations
        section of this docstring for more info.
    """

    def __init__(
        self,
        storage_key: ResourceIdentifier,
        required_tenant_types: list[str],
        default_config_overrides: dict[str, Any],
        **kwargs: str,
    ) -> None:
        self._required_tenant_types = set(required_tenant_types)
        self._resource_identifier = storage_key
        self._default_config_definitions = [
            AllocationPolicyConfig(
                name=IS_ACTIVE,
                description="Toggles whether or not this policy is active. If active, policy code will be excecuted. If inactive, the policy code will not run and the query will pass through.",
                value_type=int,
                default=default_config_overrides.get(IS_ACTIVE, 1),
            ),
            AllocationPolicyConfig(
                name=IS_ENFORCED,
                description="Toggles whether or not this policy is enforced. If enforced, policy will be able to throttle/reject incoming queries. If not enforced, this policy will not throttle/reject queries if policy is triggered, but all the policy code will still run.",
                value_type=int,
                default=default_config_overrides.get(IS_ENFORCED, 1),
            ),
            AllocationPolicyConfig(
                name=MAX_THREADS,
                description="The max threads Clickhouse can use for the query.",
                value_type=int,
                default=default_config_overrides.get(MAX_THREADS, 10),
            ),
        ]
        self._overridden_additional_config_definitions = (
            self._get_overridden_additional_config_defaults(default_config_overrides)
        )

    @classmethod
    def create_minimal_instance(
        cls, resource_identifier: str
    ) -> "ConfigurableComponent":
        return cls(
            storage_key=ResourceIdentifier(resource_identifier),
            required_tenant_types=[],
            default_config_overrides={},
        )

    @classmethod
    def component_namespace(self) -> str:
        return "AllocationPolicy"

    def _get_hash(self) -> str:
        return CAPMAN_HASH

    @property
    def metrics(self) -> MetricsWrapper:
        return MetricsWrapper(
            environment.metrics,
            "allocation_policy",
            tags={
                "storage_key": self._resource_identifier.value,
                "is_enforced": str(self.is_enforced),
                "policy_class": self.__class__.__name__,
            },
        )

    @property
    def is_active(self) -> bool:
        return bool(self.get_config_value(IS_ACTIVE)) and settings.ALLOCATION_POLICY_ENABLED

    @property
    def is_enforced(self) -> bool:
        return bool(self.get_config_value(IS_ENFORCED))

    @property
    def max_threads(self) -> int:
        """Maximum number of threads run a single query on ClickHouse with."""
        return int(self.get_config_value(MAX_THREADS))

    @classmethod
    def get_from_name(cls, name: str) -> Type["AllocationPolicy"]:
        return cast(Type["AllocationPolicy"], cls.class_from_name(name))

    def __eq__(self, other: Any) -> bool:
        """There should not be a need to compare these except that
        AllocationPolicies are attached to the Table a query is executed against.
        In order to allow that comparison, this function is implemented here.
        """
        return (
            bool(self.__class__ == other.__class__)
            and self._resource_identifier == other._resource_identifier
            and self._required_tenant_types == other._required_tenant_types
        )

    def is_cross_org_query(self, tenant_ids: dict[str, str | int]) -> bool:
        return bool(tenant_ids.get("cross_org_query", False))

    @classmethod
    def from_kwargs(cls, **kwargs: str) -> "AllocationPolicy":
        required_tenant_types = kwargs.pop("required_tenant_types", None)
        storage_key = kwargs.pop("storage_key", None)
        default_config_overrides: dict[str, Any] = cast(
            "dict[str, Any]", kwargs.pop("default_config_overrides", {})
        )
        assert isinstance(
            required_tenant_types, list
        ), "required_tenant_types must be a list of strings"
        assert isinstance(storage_key, str)
        return cls(
            required_tenant_types=required_tenant_types,
            storage_key=ResourceIdentifier(StorageKey(storage_key)),
            default_config_overrides=default_config_overrides,
            **kwargs,
        )

    def additional_config_definitions(self) -> list[Configuration]:
        return self._overridden_additional_config_definitions

    def _get_default_config_definitions(self) -> list[Configuration]:
        return cast(list[Configuration], self._default_config_definitions)

    def get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        try:
            if not self.is_active:
                allowance = QuotaAllowance(
                    can_run=True,
                    max_threads=self.max_threads,
                    explanation={},
                    is_throttled=False,
                    throttle_threshold=MAX_THRESHOLD,
                    rejection_threshold=MAX_THRESHOLD,
                    quota_used=0,
                    quota_unit=NO_UNITS,
                    suggestion=NO_SUGGESTION,
                )
            else:
                allowance = self._get_quota_allowance(tenant_ids, query_id)
        except InvalidTenantsForAllocationPolicy as e:
            allowance = QuotaAllowance(
                can_run=False,
                max_threads=0,
                explanation=cast(dict[str, Any], e.to_dict()),
                is_throttled=False,
                throttle_threshold=0,
                rejection_threshold=0,
                quota_used=0,
                quota_unit=NO_UNITS,
                suggestion=NO_SUGGESTION,
            )
        except Exception:
            logger.exception(
                "Allocation policy failed to get quota allowance, this is a bug, fix it"
            )
            if settings.RAISE_ON_ALLOCATION_POLICY_FAILURES:
                raise
            return DEFAULT_PASSTHROUGH_POLICY.get_quota_allowance(tenant_ids, query_id)
        if not allowance.can_run:
            self.metrics.increment(
                "db_request_rejected",
                tags={"referrer": str(tenant_ids.get("referrer", "no_referrer"))},
            )
        elif allowance.max_threads < self.max_threads:
            # NOTE: The elif is very intentional here. Don't count the throttling
            # if the request was rejected.
            self.metrics.increment(
                "db_request_throttled",
                tags={
                    "referrer": str(tenant_ids.get("referrer", "no_referrer")),
                    "max_threads": str(allowance.max_threads),
                },
            )
        if not self.is_enforced:
            return QuotaAllowance(
                can_run=True,
                max_threads=self.max_threads,
                explanation={},
                is_throttled=allowance.is_throttled,
                throttle_threshold=allowance.throttle_threshold,
                rejection_threshold=allowance.rejection_threshold,
                quota_used=allowance.quota_used,
                quota_unit=allowance.quota_unit,
                suggestion=allowance.suggestion,
            )
        # make sure we always know which storage key we rejected a query from
        allowance.explanation["storage_key"] = self._resource_identifier.value
        return allowance

    @abstractmethod
    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        pass

    def update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        query_id: str,
        result_or_error: QueryResultOrError,
    ) -> None:
        try:
            if not self.is_active:
                return
            return self._update_quota_balance(tenant_ids, query_id, result_or_error)
        except InvalidTenantsForAllocationPolicy:
            # the policy did not do anything because the tenants were invalid, updating is also not necessary
            pass
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
        query_id: str,
        result_or_error: QueryResultOrError,
    ) -> None:
        pass

    @property
    def resource_identifier(self) -> ResourceIdentifier:
        return self._resource_identifier

    @property
    def query_type(self) -> QueryType:
        return QueryType.SELECT

    def to_dict(self) -> PolicyData:
        base_data = super().to_dict()
        return PolicyData(**base_data, query_type=self.query_type.value)  # type: ignore


class PassthroughPolicy(AllocationPolicy):
    def _additional_config_definitions(self) -> list[Configuration]:
        return []

    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        return QuotaAllowance(
            can_run=True,
            max_threads=self.max_threads,
            explanation={},
            is_throttled=False,
            throttle_threshold=MAX_THRESHOLD,
            rejection_threshold=MAX_THRESHOLD,
            quota_used=0,
            quota_unit=NO_UNITS,
            suggestion=NO_SUGGESTION,
        )

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        query_id: str,
        result_or_error: QueryResultOrError,
    ) -> None:
        pass


DEFAULT_PASSTHROUGH_POLICY = PassthroughPolicy(
    ResourceIdentifier(StorageKey("default.no_storage_key")),
    required_tenant_types=[],
    default_config_overrides={},
)

import_submodules_in_directory(
    os.path.dirname(os.path.realpath(__file__)), "snuba.query.allocation_policies"
)
