from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from typing import Any, cast

from snuba import settings
from snuba.datasets.storages.storage_key import StorageKey
from snuba.state import delete_config as delete_runtime_config
from snuba.state import get_all_configs as get_all_runtime_configs
from snuba.state import get_config as get_runtime_config
from snuba.state import set_config as set_runtime_config
from snuba.utils.registered_class import RegisteredClass, import_submodules_in_directory
from snuba.utils.serializable_exception import JsonSerializable, SerializableException
from snuba.web import QueryException, QueryResult

logger = logging.getLogger("snuba.query.allocation_policy_base")

CAPMAN_HASH = "capman"
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
    value_type: type
    default: Any
    params: dict[str, type] = field(default_factory=dict)

    def __to_base_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "type": self.value_type,
            "default": self.default,
            "description": self.description,
        }

    def to_definition_dict(self) -> dict[str, Any]:
        """Returns a dict representation of the definition of a Config."""
        return {
            **self.__to_base_dict(),
            "params": [
                {"name": param, "type": self.params[param]} for param in self.params
            ],
        }

    def to_config_dict(self, value: Any, params: dict[str, Any]) -> dict[str, Any]:
        """Returns a dict representation of a live Config."""
        return {**self.__to_base_dict(), "value": value, "params": params}


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
        self._default_config_definitions = [
            AllocationPolicyConfig(
                name=IS_ACTIVE,
                description="Whether or not this policy is active.",
                value_type=int,
                default=1,
            ),
            AllocationPolicyConfig(
                name=IS_ENFORCED,
                description="Whether or not this policy is enforced.",
                value_type=int,
                default=0,
            ),
        ]

    @property
    def runtime_config_prefix(self) -> str:
        return f"{self._storage_key.value}.{self.__class__.__name__}"

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
    def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
        """
        Define policy specific config definitions, these will be used along
        with the default definitions of the base class. (is_enforced, is_active)
        """
        pass

    def config_definitions(self) -> dict[str, AllocationPolicyConfig]:
        """Returns a dictionary of configurable params on this AllocationPolicy."""
        return {
            config.name: config
            for config in self._default_config_definitions
            + self._additional_config_definitions()
        }

    def get_parameterized_config_definitions(self) -> list[dict[str, Any]]:
        """Returns a dictionary of parameterized configs on this AllocationPolicy."""
        return [
            definition.to_definition_dict()
            for definition in self.config_definitions().values()
            if definition.params
        ]

    def __build_runtime_config_key(self, config: str, params: dict[str, Any]) -> str:
        """
        Example return values:
        - `mystorage.MyAllocationPolicy.my_config`            # no params
        - `mystorage.MyAllocationPolicy.my_config.a:1,b:2`    # sorted params
        """
        parameters = "."
        for param in sorted(list(params.keys())):
            parameters += f"{param}:{params[param]},"
        parameters = parameters[:-1]
        return f"{self.runtime_config_prefix}.{config}{parameters}"

    def __validate_config_params(
        self, config_key: str, params: dict[str, Any], value: Any = None
    ) -> AllocationPolicyConfig:
        definitions = self.config_definitions()

        # config doesn't exist
        if config_key not in definitions:
            raise InvalidPolicyConfig(
                f"'{config_key}' is not a valid config for {self.__class__.__name__}!"
            )

        config = definitions[config_key]

        # missing required parameters
        if (
            diff := {
                key: config.params[key].__name__
                for key in config.params
                if key not in params
            }
        ) != dict():
            raise InvalidPolicyConfig(
                f"'{config_key}' missing required parameters: {diff} for {self.__class__.__name__}!"
            )

        # parameters aren't correct types
        if params:
            for param in params:
                if not isinstance(params[param], config.params[param]):
                    raise InvalidPolicyConfig(
                        f"'{config_key}' parameter '{param}' needs to be of type"
                        f" {config.params[param].__name__} (not {type(params[param]).__name__})"
                        f" for {self.__class__.__name__}!"
                    )

        # value isn't correct type
        if value is not None:
            if not isinstance(value, config.value_type):
                raise InvalidPolicyConfig(
                    f"'{config_key}' value needs to be of type"
                    f" {config.value_type.__name__} (not {type(value).__name__})"
                    f" for {self.__class__.__name__}!"
                )

        return config

    def get_config(
        self, config_key: str, params: dict[str, Any] = {}, validate: bool = True
    ) -> Any:
        """Returns value of a config on this Allocation Policy, or the default if none exists in Redis."""
        config_definition = (
            self.__validate_config_params(config_key, params)
            if validate
            else self.config_definitions()[config_key]
        )
        return cast(
            config_definition.value_type,  # type: ignore
            get_runtime_config(
                key=self.__build_runtime_config_key(config_key, params),
                default=config_definition.default,
                config_key=CAPMAN_HASH,
            ),
        )

    def set_config(
        self,
        config_key: str,
        value: Any,
        params: dict[str, Any] = {},
        user: str | None = None,
    ) -> None:
        """Sets a value of a config on this AllocationPolicy."""
        self.__validate_config_params(config_key, params, value)
        set_runtime_config(
            key=self.__build_runtime_config_key(config_key, params),
            value=value,
            user=user,
            config_key=CAPMAN_HASH,
        )

    def delete_config(
        self,
        config_key: str,
        params: dict[str, Any] = {},
        user: str | None = None,
    ) -> None:
        """Deletes an instance of a parameterized config on this AllocationPolicy."""
        self.__validate_config_params(config_key, params)
        delete_runtime_config(
            key=self.__build_runtime_config_key(config_key, params),
            user=user,
            config_key=CAPMAN_HASH,
        )

    def get_detailed_configs(self) -> list[dict[str, Any]]:
        """Returns a list of live configs with their definitions on this AllocationPolicy."""

        configs = get_all_runtime_configs(CAPMAN_HASH)
        definitions = self.config_definitions()
        detailed_configs: list[dict[str, Any]] = []

        for key in configs:
            if key.startswith(self.runtime_config_prefix):
                # key is "storage.policy.config" or "storage.policy.config.param1:val1,param2:val2"
                _, _, config_key, *params = key.split(".")
                # (config_key, params) is ("config", []) or ("config", ["param1:val1,param2:val2"])
                params_dict = dict()
                if params:
                    # convert ["param1:val1,param2:val2"] to {"param1": "val1", "param2": "val2"}
                    [params_string] = params
                    params_split = params_string.split(",")
                    for param_string in params_split:
                        param_key, param_value = param_string.split(":")
                        params_dict[param_key] = param_value
                detailed_configs.append(
                    definitions[config_key].to_config_dict(
                        value=configs[key], params=params_dict
                    )
                )

        return detailed_configs

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
    def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
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
