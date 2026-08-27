from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, cast

from redis.exceptions import TimeoutError as RedisTimeoutError
from sentry_sdk import traces

from snuba import environment, settings
from snuba.configs.configuration import (
    ConfigurableComponent,
    ConfigurableComponentData,
    Configuration,
    ResourceIdentifier,
    logger,
)
from snuba.datasets.storages.storage_key import StorageKey
from snuba.state.sentry_options import get_mapped_option
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.registered_class import InvalidConfigKeyError, import_submodules_in_directory
from snuba.utils.sentry import SENTRY_OP
from snuba.utils.serializable_exception import JsonSerializable, SerializableException
from snuba.web import QueryResult

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
    error: Exception | None

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
    ) -> InvalidTenantsForAllocationPolicy:
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
        return {k: v for k, v in details.items() if not v["can_run"]}

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
    ) -> AllocationPolicyViolations:
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
        >>>         # with the default definitions of the base class. (is_enforced)
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
        >>>     StorageKey("mystorage"),
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

    AllocationPolicy Configurations are a way to update live flags without shipping code changes. Values are
    sourced from the ``configurable_component_overrides`` sentry-option (then each config's code default).

    Any configuration definition that exists in your sub class' `_additional_config_definitions()` will appear in the
    Capacity Management Snuba Admin UI for the policy. From there you can modify the live values to alter how your policy works.

    The base class comes with a built in config accessible as a property of the class itself:
    - is_enforced
        - Use this to throttle/reject queries OR just log stuff. A configured policy is always active.

    Eg.

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
        >>>     # kwargs override whatever defaults are set for this class
        >>>     is_enforced=False,
        >>>     fart_noise_level=100,
        >>> )


    NOTE:
    - Use the `self.get_config_value()` methods on the policy itself for policy-specific config. Global
        toggles live as sentry-options under the ``snuba`` namespace.
    - If for some reason you find yourself needing a new global option, consider adding it as a property to
        this base class since it should be universally useful across policies. An example of this is the `max_threads` property.


    **GOTCHAS**
    -----------

    * Because allocation policies are attached to query objects, they have to be pickleable. Don't put non-pickleable members onto the allocation policy
    * At time of writing (29-03-2023), not all allocation policy decisions are made in the allocation policy,
        table rate limiters are still applied in the query pipeline, those should be moved into an allocation policy as they
        are also policy decisions
    * Every allocation policy takes a `storage_key` in its init. The storage_key is like a pseudo-tenant. In different
        environments, storages may be co-located on the same cluster. To facilitate resource sharing, every allocation policy
        knows which storage_key it is serving. This is used to create unique keys for config values.
        See `_build_config_key()` for more info.
    """

    required_tenant_types: frozenset[str] = frozenset()

    def __init__(
        self,
        storage_key: ResourceIdentifier,
        **kwargs: Any,
    ) -> None:
        self._resource_identifier = storage_key
        self._default_config_definitions = [
            AllocationPolicyConfig(
                name=IS_ENFORCED,
                description="Toggles whether or not this policy is enforced. If enforced, policy will be able to throttle/reject incoming queries. If not enforced, this policy will not throttle/reject queries if policy is triggered, but all the policy code will still run.",
                value_type=int,
                default=kwargs.get(IS_ENFORCED, 1),
            ),
            AllocationPolicyConfig(
                name=MAX_THREADS,
                description="The max threads Clickhouse can use for the query.",
                value_type=int,
                default=kwargs.get(MAX_THREADS, 10),
            ),
        ]
        self._overridden_additional_config_definitions = (
            self._get_overridden_additional_config_defaults(kwargs)
        )

    @classmethod
    def create_minimal_instance(cls, resource_identifier: str) -> ConfigurableComponent:
        return cls(
            storage_key=ResourceIdentifier(resource_identifier),
        )

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
    def is_enforced(self) -> bool:
        return bool(self.get_config_value(IS_ENFORCED))

    @property
    def max_threads(self) -> int:
        """Maximum number of threads run a single query on ClickHouse with."""
        return int(self.get_config_value(MAX_THREADS))

    def __eq__(self, other: Any) -> bool:
        """There should not be a need to compare these except that
        AllocationPolicies are attached to the Table a query is executed against.
        In order to allow that comparison, this function is implemented here.
        """
        return (
            bool(self.__class__ == other.__class__)
            and self._resource_identifier == other._resource_identifier
        )

    def is_cross_org_query(self, tenant_ids: dict[str, str | int]) -> bool:
        return bool(tenant_ids.get("cross_org_query", False))

    @classmethod
    def from_kwargs(
        cls,
        *,
        storage_key: str,
        **kwargs: Any,
    ) -> AllocationPolicy:
        return cls(
            storage_key=ResourceIdentifier(StorageKey(storage_key)),
            **kwargs,
        )

    def additional_config_definitions(self) -> list[Configuration]:
        return self._overridden_additional_config_definitions

    def _get_default_config_definitions(self) -> list[Configuration]:
        return cast(list[Configuration], self._default_config_definitions)

    def get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        with traces.start_span(
            name=self.__class__.__name__,
            attributes={SENTRY_OP: "allocation_policy.get_quota_allowance"},
        ) as span:
            for t, tid in tenant_ids.items():
                span.set_attribute(f"tenant_ids.{t}", str(tid))
            try:
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
            except (RedisTimeoutError, StopIteration) as e:
                # Expected transient errors (Redis timeouts, unexpected pipeline
                # result counts). Fail open to avoid blocking requests.
                self.metrics.increment(
                    "fail_open",
                    1,
                    tags={"method": "get_quota_allowance", "reason": type(e).__name__},
                )
                return DEFAULT_PASSTHROUGH_POLICY.get_quota_allowance(tenant_ids, query_id)
            except Exception:
                self.metrics.increment("fail_open", 1, tags={"method": "get_quota_allowance"})
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
                span.set_attribute("db_request_throttled", True)
            if not self.is_enforced:
                allowance = QuotaAllowance(
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
            for k, v in allowance.to_dict().items():
                # Attributes only accept scalars; stringify nested values.
                span.set_attribute(
                    f"quota_allowance.{k}",
                    v if isinstance(v, (str, int, float, bool)) else json.dumps(v, default=repr),
                )
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
            return self._update_quota_balance(tenant_ids, query_id, result_or_error)
        except InvalidTenantsForAllocationPolicy:
            # the policy did not do anything because the tenants were invalid, updating is also not necessary
            pass
        except (RedisTimeoutError, StopIteration) as e:
            self.metrics.increment(
                "fail_open", 1, tags={"method": "update_quota_balance", "reason": type(e).__name__}
            )
        except Exception:
            self.metrics.increment("fail_open", 1, tags={"method": "update_quota_balance"})
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
        return PolicyData(**base_data, query_type=self.query_type.value)


class PassthroughPolicy(AllocationPolicy):
    required_tenant_types: frozenset[str] = frozenset()

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


ALLOCATION_POLICY_KEY = "allocation_policies"


def _default_passthough_policy(storage_key: str = "default.no_storage_key") -> AllocationPolicy:
    return PassthroughPolicy(
        ResourceIdentifier(StorageKey(storage_key)),
    )


DEFAULT_PASSTHROUGH_POLICY = _default_passthough_policy()


def get_active_allocation_policies(
    resource_identifier: ResourceIdentifier,
) -> list[AllocationPolicy]:
    """Build the AllocationPolicy list configured for ``resource_identifier``.

    Reads the ``allocation_policies`` sentry-option (keyed by ResourceIdentifier
    value). Each resource is a list of match blocks; this reader uses the first
    block's ``policies`` list (``match`` is ignored). An absent or empty entry
    falls back to a PassthroughPolicy for that resource. Tenant types live on
    the policy class, not the option. Per-policy settings on the item
    (is_enforced, concurrent_limit, …) are passed as constructor kwargs.
    """
    policies: list[AllocationPolicy] = []
    blocks: list[Mapping[str, Any]] = get_mapped_option(
        ALLOCATION_POLICY_KEY, resource_identifier.value, []
    )
    first = blocks[0] if blocks else {}
    specs: list[Any] = first.get("policies", []) if isinstance(first, dict) else []
    if not isinstance(specs, list):
        logger.warning(
            "Ignoring allocation_policy block without policies list for %s: %r",
            resource_identifier.value,
            first,
        )
        specs = []

    for spec in specs:
        if not isinstance(spec, dict):
            logger.warning(
                "Ignoring malformed allocation_policy entry for %s: %r",
                resource_identifier.value,
                spec,
            )
            continue

        name = spec.get("name", None)
        if not isinstance(name, str):
            logger.warning(
                "Ignoring allocation_policy entry without name for %s: %r",
                resource_identifier.value,
                spec,
            )
            continue

        try:
            policies.append(
                AllocationPolicy.get_from_name(name).from_kwargs(
                    storage_key=resource_identifier.value,
                    **spec,
                )
            )
        except InvalidConfigKeyError:
            logger.warning(
                "Unknown allocation policy %s for %s",
                name,
                resource_identifier.value,
            )

    return policies or [_default_passthough_policy(resource_identifier.value)]


import_submodules_in_directory(
    os.path.dirname(os.path.realpath(__file__)), "snuba.query.allocation_policies"
)
