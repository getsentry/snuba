from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import asdict, dataclass, replace
from enum import IntEnum, StrEnum
from typing import Any, assert_never, cast

from redis.exceptions import TimeoutError as RedisTimeoutError
from sentry_sdk import traces

from snuba import environment, settings
from snuba.clusters.load_info import LoadInfo
from snuba.configs.configuration import ResourceIdentifier, logger
from snuba.datasets.storages.storage_key import StorageKey
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.registered_class import RegisteredClass, import_submodules_in_directory
from snuba.utils.sentry import SENTRY_OP
from snuba.utils.serializable_exception import JsonSerializable, SerializableException
from snuba.web import QueryResult

DEFAULT_MAX_THREADS = 10
NO_UNITS = "no_units"
NO_SUGGESTION = "no_suggestion"
CROSS_ORG_SUGGESTION = "cross org queries do not have limits"
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


class QuotaAllowanceDecision(IntEnum):
    REJECTED = 0
    PARDONED = 1
    ALLOWED = 2

    @property
    def is_rejected(self) -> bool:
        return self is QuotaAllowanceDecision.REJECTED


@dataclass(frozen=True)
class QuotaAllowance:
    can_run: bool
    max_threads: int
    # if any limiting action was taken by the allocation
    # policy, this dictionary should contain some information
    # about what caused that action. Not currently well typed
    # because I don't know what exactly should go in it yet
    explanation: dict[str, JsonSerializable]
    rejection_threshold: int
    quota_used: int
    quota_unit: str
    suggestion: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def decision(
        self,
        policy: AllocationPolicy,
        load_info: LoadInfo | None,
    ) -> QuotaAllowanceDecision:
        if not self.can_run:
            # Pardon a rejection only when the policy opts in, we have load info,
            # and the cluster is below all reject ceilings. Empty thresholds never
            # exceed, so they pardon. Missing load info => keep rejecting.
            reject_thresholds = policy.thresholds.get("reject") or {}
            idle_pardon = (
                policy.is_pardonable and load_info and not load_info.exceeds(reject_thresholds)
            )
            return (
                QuotaAllowanceDecision.PARDONED if idle_pardon else QuotaAllowanceDecision.REJECTED
            )
        return QuotaAllowanceDecision.ALLOWED


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


class Enforce(StrEnum):
    ALWAYS = "always"
    PARDON = "pardon"
    NEVER = "never"


class AllocationPolicy(ABC, metaclass=RegisteredClass):
    """Decides whether a query on a resource can run, and with how many threads.

    Subclasses implement two hooks: `_get_quota_allowance` before the query runs and
    `_update_quota_balance` after it. Policies are configured per resource through
    the `allocation_policies` sentry-option (see resolver.py).

    The hooks fail open: an unexpected error or a Redis timeout never blocks a query
    (a bug raises only when RAISE_ON_ALLOCATION_POLICY_FAILURES is set). Invalid
    tenant ids count as a rejection.
    """

    def __init__(
        self,
        storage_key: ResourceIdentifier,
        *,
        enforce: Enforce | str = Enforce.ALWAYS,
        max_threads: int = DEFAULT_MAX_THREADS,
        thresholds: Mapping[str, Mapping[str, float]] | None = None,
    ) -> None:
        self._resource_identifier = storage_key
        self._enforce = Enforce(enforce)
        self._max_threads = max_threads
        self._thresholds: dict[str, dict[str, float]] = {
            action: {k: float(v) for k, v in ceilings.items()}
            for action, ceilings in (thresholds or {}).items()
        }

    @classmethod
    def config_key(cls) -> str:
        return f"AllocationPolicy.{cls.__name__}"

    @classmethod
    def class_name(cls) -> str:
        return cls.__name__

    @classmethod
    def get_from_name(cls, name: str) -> type[AllocationPolicy]:
        return cast(type[AllocationPolicy], cls.class_from_name(f"AllocationPolicy.{name}"))

    def component_name(self) -> str:
        return f"{self._resource_identifier.value}.{self.__class__.__name__}"

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
        return self._enforce is not Enforce.NEVER

    @property
    def is_pardonable(self) -> bool:
        return self._enforce is Enforce.PARDON

    @property
    def thresholds(self) -> dict[str, dict[str, float]]:
        return self._thresholds

    @property
    def max_threads(self) -> int:
        """Maximum number of threads run a single query on ClickHouse with."""
        return self._max_threads

    def is_cross_org_query(self, tenant_ids: dict[str, str | int]) -> bool:
        return bool(tenant_ids.get("cross_org_query", False))

    def _tenant(self, tenant_ids: dict[str, str | int], field: str) -> str | int:
        value = tenant_ids.get(field)
        if value is None:
            raise InvalidTenantsForAllocationPolicy.from_args(
                tenant_ids, type(self).__name__, f"needs {field}"
            )
        return value

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

    def get_quota_allowance(
        self,
        tenant_ids: dict[str, str | int],
        query_id: str,
        load_info: LoadInfo | None = None,
    ) -> QuotaAllowance:
        with traces.start_span(
            name=self.__class__.__name__,
            attributes={SENTRY_OP: "allocation_policy.get_quota_allowance"},
        ) as span:
            for t, tid in tenant_ids.items():
                span.set_attribute(f"tenant_ids.{t}", str(tid))

            passthrough = _default_passthough_policy(self._resource_identifier.value)
            try:
                allowance = self._get_quota_allowance(tenant_ids, query_id)
            except InvalidTenantsForAllocationPolicy as e:
                allowance = QuotaAllowance(
                    can_run=False,
                    max_threads=0,
                    explanation=cast(dict[str, Any], e.to_dict()),
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
                return passthrough.get_quota_allowance(tenant_ids, query_id, load_info)
            except Exception:
                self.metrics.increment("fail_open", 1, tags={"method": "get_quota_allowance"})
                logger.exception(
                    "Allocation policy failed to get quota allowance, this is a bug, fix it"
                )
                if settings.RAISE_ON_ALLOCATION_POLICY_FAILURES:
                    raise
                return passthrough.get_quota_allowance(tenant_ids, query_id, load_info)

            decision = allowance.decision(self, load_info)
            decision_tags = {
                "referrer": str(tenant_ids.get("referrer", "no_referrer")),
                "org_id": str(tenant_ids.get("organization_id", "no_org_id")),
                "project_id": str(tenant_ids.get("project_id", "no_project_id")),
            }
            match decision:
                case QuotaAllowanceDecision.PARDONED:
                    self.metrics.increment("db_request_pardoned", tags=decision_tags)
                    allowance.explanation["idle_pardon"] = (
                        load_info.to_dict() if load_info is not None else {}
                    )
                case QuotaAllowanceDecision.REJECTED:
                    self.metrics.increment("db_request_rejected", tags=decision_tags)
                case QuotaAllowanceDecision.ALLOWED:
                    pass
                case unreachable:
                    assert_never(unreachable)

            if decision.is_rejected and not self.is_enforced:
                allowance = replace(
                    allowance, can_run=True, max_threads=self.max_threads, explanation={}
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
    ) -> int | None:
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
        return None

    @abstractmethod
    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        query_id: str,
        result_or_error: QueryResultOrError,
    ) -> int | None:
        pass

    @property
    def resource_identifier(self) -> ResourceIdentifier:
        return self._resource_identifier


class PassthroughPolicy(AllocationPolicy):
    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        return QuotaAllowance(
            can_run=True,
            max_threads=self.max_threads,
            explanation={},
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


import_submodules_in_directory(
    os.path.dirname(os.path.realpath(__file__)), "snuba.query.allocation_policies"
)
