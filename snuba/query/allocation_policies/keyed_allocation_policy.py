from __future__ import annotations

import inspect
from abc import abstractmethod
from collections.abc import Mapping
from enum import StrEnum
from typing import Any

from snuba.configs.configuration import Configuration, ResourceIdentifier
from snuba.query.allocation_policies import (
    CROSS_ORG_SUGGESTION,
    NO_SUGGESTION,
    AllocationPolicy,
    AllocationPolicyError,
    InvalidTenantsForAllocationPolicy,
    QueryResultOrError,
    QuotaAllowance,
)

DEFAULT_MAX_THREADS = 10


def setting[T: (int, float)](name: str, value: object, kind: type[T]) -> T:
    """Convert a config value, raising AllocationPolicyError if it is not a number."""
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise AllocationPolicyError(f"{name} must be a number, got {value!r}")
    try:
        return kind(value)
    except ValueError as err:
        raise AllocationPolicyError(f"{name} must be a number, got {value!r}") from err


class Enforce(StrEnum):
    ALWAYS = "always"
    PARDON = "pardon"
    NEVER = "never"


class KeyedAllocationPolicy(AllocationPolicy):
    """Base for keyed policies: count usage per key, reject once a key reaches `limit`.

    A concrete policy combines two parts:

    - what is counted: a subclass of this (e.g. BytesScannedPolicy) implements
      `_check` and `_record`;
    - what it is counted per: a concrete subclass of that (e.g.
      BytesScannedPerOrgPolicy) implements `_key` with one of the key functions
      below (org_key, ref_key, org_ref_key).

    Only the concrete subclasses are named in config.

    Settings come only from the allocation_policies match block, never from
    configurable_component_overrides:

    - limit: usage allowed per key, in the subclass's quota_unit.
    - enforce: always | pardon | never. pardon lets the query run while cluster
      load is under the block's thresholds.reject; never only records metrics.
      Mapped onto is_enforced / is_pardonable so AllocationPolicy and the
      callers' QuotaAllowance.decision() apply it unchanged.
    - max_threads: threads granted when the query is allowed.
    """

    def __init__(
        self,
        storage_key: ResourceIdentifier,
        *,
        limit: int | None = None,
        enforce: Enforce | str = Enforce.ALWAYS,
        max_threads: int = DEFAULT_MAX_THREADS,
        thresholds: Mapping[str, Mapping[str, float]] | None = None,
    ) -> None:
        super().__init__(storage_key, thresholds=thresholds)
        self._limit = setting("limit", self.default_limit if limit is None else limit, int)
        self._max_threads = setting("max_threads", max_threads, int)
        if self._limit <= 0:
            raise AllocationPolicyError(f"limit must be positive, got {self._limit}")
        try:
            self._enforce = Enforce(enforce)
        except ValueError as err:
            raise AllocationPolicyError(
                f"enforce must be one of {[e.value for e in Enforce]}, got {enforce!r}"
            ) from err

    @classmethod
    def from_kwargs(cls, *, storage_key: str, **kwargs: Any) -> AllocationPolicy:
        # Config can hold any key; reject the ones __init__ does not name.
        unknown = set(kwargs) - set(inspect.signature(cls.__init__).parameters)
        if unknown:
            raise AllocationPolicyError(f"{cls.__name__} has no settings {sorted(unknown)}")
        return super().from_kwargs(storage_key=storage_key, **kwargs)

    @property
    @abstractmethod
    def quota_unit(self) -> str:
        """Unit of `limit` and usage, e.g. "bytes"."""

    @property
    @abstractmethod
    def default_limit(self) -> int:
        """Limit used when the config sets none."""

    @property
    @abstractmethod
    def suggestion(self) -> str:
        """Text returned to the caller when a query is rejected."""

    @abstractmethod
    def _key(self, tenant_ids: dict[str, str | int]) -> str:
        """The counter this query belongs to."""

    @abstractmethod
    def _check(self, key: str, query_id: str) -> int:
        """Usage of `key` before the query runs. May also register the query
        (e.g. as running), which `_record` then settles."""

    @abstractmethod
    def _record(self, key: str, query_id: str, result_or_error: QueryResultOrError) -> None:
        """Account for the query against `key` once it has finished."""

    def _additional_config_definitions(self) -> list[Configuration]:
        return []

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def enforce(self) -> Enforce:
        return self._enforce

    @property
    def is_enforced(self) -> bool:
        return self._enforce is not Enforce.NEVER

    @property
    def is_pardonable(self) -> bool:
        return self._enforce is Enforce.PARDON

    @property
    def max_threads(self) -> int:
        return self._max_threads

    def _storage_prefix(self, key: str) -> str:
        """Redis prefix for `key`. component_name() is unique per storage and class."""
        return f"{self.component_name()}-{key}"

    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        if self.is_cross_org_query(tenant_ids):
            # Cross-org queries have no single org to count against.
            return self._allowance(True, 0, {"reason": "cross_org_query"}, CROSS_ORG_SUGGESTION)

        key = self._key(tenant_ids)
        used = self._check(key, query_id)
        if used < self._limit:
            return self._allowance(True, used, {"reason": "within_limit"}, NO_SUGGESTION)
        return self._allowance(
            False,
            used,
            {
                "reason": f"{key} is at {used} of its {self._limit} {self.quota_unit} limit",
                "limit": self._limit,
            },
            self.suggestion,
        )

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        query_id: str,
        result_or_error: QueryResultOrError,
    ) -> None:
        if self.is_cross_org_query(tenant_ids):
            return
        self._record(self._key(tenant_ids), query_id, result_or_error)

    def _allowance(
        self, can_run: bool, used: int, explanation: dict[str, Any], suggestion: str
    ) -> QuotaAllowance:
        return QuotaAllowance(
            can_run=can_run,
            max_threads=self._max_threads if can_run else 0,
            explanation=explanation,
            is_throttled=False,
            throttle_threshold=self._limit,
            rejection_threshold=self._limit,
            quota_used=used,
            quota_unit=self.quota_unit,
            suggestion=suggestion,
        )


def _tenant(tenant_ids: dict[str, str | int], field: str, policy: object) -> str | int:
    value = tenant_ids.get(field)
    if value is None:
        raise InvalidTenantsForAllocationPolicy.from_args(
            tenant_ids, type(policy).__name__, f"needs {field}"
        )
    return value


def org_key(tenant_ids: dict[str, str | int], policy: object) -> str:
    """One counter per organization, across all its referrers."""
    return f"org:{_tenant(tenant_ids, 'organization_id', policy)}"


def ref_key(tenant_ids: dict[str, str | int], policy: object) -> str:
    """One counter per referrer, shared by all organizations."""
    return f"ref:{_tenant(tenant_ids, 'referrer', policy)}"


def org_ref_key(tenant_ids: dict[str, str | int], policy: object) -> str:
    """One counter per organization and referrer."""
    return f"{org_key(tenant_ids, policy)}|{ref_key(tenant_ids, policy)}"
