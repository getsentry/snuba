from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field, replace
from typing import Any, cast

from snuba import environment, settings
from snuba.datasets.storages.storage_key import StorageKey
from snuba.state import delete_config as delete_runtime_config
from snuba.state import get_all_configs as get_all_runtime_configs
from snuba.state import get_config as get_runtime_config
from snuba.state import set_config as set_runtime_config
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.registered_class import RegisteredClass, import_submodules_in_directory
from snuba.utils.serializable_exception import JsonSerializable, SerializableException
from snuba.web import QueryException, QueryResult

logger = logging.getLogger("snuba.query.allocation_policy_base")
CAPMAN_PREFIX = "capman"

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
    param_types: dict[str, type] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if type(self.default) != self.value_type:
            raise ValueError(
                f"Config item `{self.name}` expects type {self.value_type} got value `{self.default}` of type {type(self.default)}"
            )

    def __to_base_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "type": self.value_type.__name__,
            "default": self.default,
            "description": self.description,
        }

    def to_definition_dict(self) -> dict[str, Any]:
        """Returns a dict representation of the definition of a Config."""
        return {
            **self.__to_base_dict(),
            "params": [
                {"name": param, "type": self.param_types[param].__name__}
                for param in self.param_types
            ],
        }

    def to_config_dict(
        self, value: Any = None, params: dict[str, Any] = {}
    ) -> dict[str, Any]:
        """Returns a dict representation of a live Config."""
        return {
            **self.__to_base_dict(),
            "value": value or self.default,
            "params": params,
        }


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
    ===================================

        >>> class MyAllocationPolicy(AllocationPolicy):

        >>>    def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
        >>>         # Define policy specific config definitions, these will be used along
        >>>         # with the default definitions of the base class. (is_enforced, is_active)
        >>>         pass

        >>>     # Use your configs in the following methods

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
        >>>     StorageKey("mystorage"), required_tenant_types=["organization_id", "referrer"]
        >>> )
        >>> allowance = policy.get_quota_allowance({"organization_id": 1234, "referrer": "myreferrer"})
        >>> result = run_db_query(allowance)
        >>> policy.update_quota_balance(
        >>>     tenant_ids={"organization_id": 1234, "referrer": "myreferrer"},
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
        rate limiters are still applied in the query pipeline, those should be moved into an allocation policy as they
        are also policy decisions
    * get_quota_allowance will throw an AllocationPolicyViolation if _get_quota_allowance().can_run is false.
        this is to keep with the pattern in `db_query.py` which communicates error states with exceptions. There is no other
        reason. For more information see snuba.web.db_query.db_query
    * Every allocation policy takes a `storage_key` in its init. The storage_key is like a pseudo-tenant. In different
        environments, storages may be co-located on the same cluster. To facilitate resource sharing, every allocation policy
        knows which storage_key it is serving. This is used to create unique keys for saving the config values.
        See `__build_runtime_config_key()` for more info.
    * Reiterating that you should no longer use `snuba.state.{get,set}_config()` for runtime configs for a specific Policy. Refer to the Configurations
        section of this docstring for more info.
    """

    def __init__(
        self,
        storage_key: StorageKey,
        required_tenant_types: list[str],
        default_config_overrides: dict[str, Any],
        **kwargs: str,
    ) -> None:
        self._required_tenant_types = set(required_tenant_types)
        self._storage_key = storage_key
        self._default_config_definitions = [
            AllocationPolicyConfig(
                name=IS_ACTIVE,
                description="Whether or not this policy is active.",
                value_type=int,
                default=default_config_overrides.get(IS_ACTIVE, 1),
            ),
            AllocationPolicyConfig(
                name=IS_ENFORCED,
                description="Whether or not this policy is enforced.",
                value_type=int,
                default=default_config_overrides.get(IS_ENFORCED, 1),
            ),
        ]
        self._overridden_additional_config_definitions = (
            self.__get_overridden_additional_config_defaults(default_config_overrides)
        )

    @property
    def metrics(self) -> MetricsWrapper:
        return MetricsWrapper(
            environment.metrics,
            "allocation_policy",
            tags={
                "storage_key": self._storage_key.value,
                "is_enforced": str(self.is_enforced),
                "policy_class": self.__class__.__name__,
            },
        )

    @property
    def runtime_config_prefix(self) -> str:
        return f"{self._storage_key.value}.{self.__class__.__name__}"

    @property
    def is_active(self) -> bool:
        return bool(self.get_config_value(IS_ACTIVE))

    @property
    def is_enforced(self) -> bool:
        return bool(self.get_config_value(IS_ENFORCED))

    @property
    def max_threads(self) -> int:
        """Maximum number of threads run a single query on ClickHouse with."""
        return cast(int, get_runtime_config("query_settings/max_threads", 8))

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
        default_config_overrides: dict[str, Any] = cast(
            "dict[str, Any]", kwargs.pop("default_config_overrides", {})
        )
        assert isinstance(
            required_tenant_types, list
        ), "required_tenant_types must be a list of strings"
        assert isinstance(storage_key, str)
        return cls(
            required_tenant_types=required_tenant_types,
            storage_key=StorageKey(storage_key),
            default_config_overrides=default_config_overrides,
            **kwargs,
        )

    def __get_overridden_additional_config_defaults(
        self, default_config_overrides: dict[str, Any]
    ) -> list[AllocationPolicyConfig]:
        """overrides the defaults specified for the config in code with the default specified
        to the instance of the policy
        """
        definitions = self._additional_config_definitions()
        return [
            replace(
                definition,
                default=default_config_overrides.get(
                    definition.name, definition.default
                ),
            )
            for definition in definitions
        ]

    def additional_config_definitions(self) -> list[AllocationPolicyConfig]:
        return self._overridden_additional_config_definitions

    @abstractmethod
    def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
        """
        Define policy specific config definitions, these will be used along
        with the default definitions of the base class. (is_enforced, is_active)
        """
        pass

    def config_definitions(self) -> dict[str, AllocationPolicyConfig]:
        """Returns a dictionary of config definitions on this AllocationPolicy."""
        return {
            config.name: config
            for config in self._default_config_definitions
            + self.additional_config_definitions()
        }

    def get_optional_config_definitions_json(self) -> list[dict[str, Any]]:
        """Returns a json-like dictionary of optional config definitions on this AllocationPolicy."""
        return [
            definition.to_definition_dict()
            for definition in self.config_definitions().values()
            if definition.param_types
        ]

    def get_config_value(
        self, config_key: str, params: dict[str, Any] = {}, validate: bool = True
    ) -> Any:
        """Returns value of a config on this Allocation Policy, or the default if none exists in Redis."""
        config_definition = (
            self.__validate_config_params(config_key, params)
            if validate
            else self.config_definitions()[config_key]
        )
        return get_runtime_config(
            key=self.__build_runtime_config_key(config_key, params),
            default=config_definition.default,
            config_key=CAPMAN_HASH,
        )

    def set_config_value(
        self,
        config_key: str,
        value: Any,
        params: dict[str, Any] = {},
        user: str | None = None,
    ) -> None:
        """Sets a value of a config on this AllocationPolicy."""
        config_definition = self.__validate_config_params(config_key, params, value)
        # ensure correct type is stored
        value = config_definition.value_type(value)
        set_runtime_config(
            key=self.__build_runtime_config_key(config_key, params),
            value=value,
            user=user,
            force=True,
            config_key=CAPMAN_HASH,
        )

    def delete_config_value(
        self,
        config_key: str,
        params: dict[str, Any] = {},
        user: str | None = None,
    ) -> None:
        """
        Deletes an instance of an optional config on this AllocationPolicy.
        If this function is run on a required config, it resets the value to default instead.
        """
        self.__validate_config_params(config_key, params)
        delete_runtime_config(
            key=self.__build_runtime_config_key(config_key, params),
            user=user,
            config_key=CAPMAN_HASH,
        )

    def get_current_configs(self) -> list[dict[str, Any]]:
        """Returns a list of live configs with their definitions on this AllocationPolicy."""

        runtime_configs = get_all_runtime_configs(CAPMAN_HASH)
        definitions = self.config_definitions()

        required_configs = set(
            config_name
            for config_name, config_def in definitions.items()
            if not config_def.param_types
        )

        detailed_configs: list[dict[str, Any]] = []

        for key in runtime_configs:
            if key.startswith(self.runtime_config_prefix):
                try:
                    config_key, params = self.__deserialize_runtime_config_key(key)
                except Exception:
                    logger.exception(
                        f"AllocationPolicy could not deserialize a key: {key}"
                    )
                    continue
                detailed_configs.append(
                    definitions[config_key].to_config_dict(
                        value=runtime_configs[key], params=params
                    )
                )
                if config_key in required_configs:
                    required_configs.remove(config_key)

        for required_config_key in required_configs:
            detailed_configs.append(definitions[required_config_key].to_config_dict())

        return detailed_configs

    def __build_runtime_config_key(self, config: str, params: dict[str, Any]) -> str:
        """
        Builds a unique key to be used in the actual datastore containing these configs.

        Example return values:
        - `"mystorage.MyAllocationPolicy.my_config"`            # no params
        - `"mystorage.MyAllocationPolicy.my_config.a:1,b:2"`    # sorted params
        """
        parameters = "."
        for param in sorted(list(params.keys())):
            parameters += f"{param}:{params[param]},"
        parameters = parameters[:-1]
        return f"{self.runtime_config_prefix}.{config}{parameters}"

    def __deserialize_runtime_config_key(self, key: str) -> tuple[str, dict[str, Any]]:
        """
        Given a raw runtime config key, deconstructs it into it's AllocationPolicy config
        key and parameters components.

        Examples:
        - `"mystorage.MyAllocationPolicy.my_config"`
            - returns `"my_config", {}`
        - `"mystorage.MyAllocationPolicy.my_config.a:1,b:2"`
            - returns `"my_config", {"a": 1, "b": 2}`
        """

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

        self.__validate_config_params(config_key=config_key, params=params_dict)

        return config_key, params_dict

    def __validate_config_params(
        self, config_key: str, params: dict[str, Any], value: Any = None
    ) -> AllocationPolicyConfig:
        definitions = self.config_definitions()

        class_name = self.__class__.__name__

        # config doesn't exist
        if config_key not in definitions:
            raise InvalidPolicyConfig(
                f"'{config_key}' is not a valid config for {class_name}!"
            )

        config = definitions[config_key]

        # missing required parameters
        if (
            diff := {
                key: config.param_types[key].__name__
                for key in config.param_types
                if key not in params
            }
        ) != dict():
            raise InvalidPolicyConfig(
                f"'{config_key}' missing required parameters: {diff} for {class_name}!"
            )

        # not an optional config (no parameters)
        if params and not config.param_types:
            raise InvalidPolicyConfig(
                f"'{config_key}' takes no params for {class_name}!"
            )

        # parameters aren't correct types
        if params:
            for param_name in params:
                if not isinstance(params[param_name], config.param_types[param_name]):
                    try:
                        # try casting to the right type, eg try int("10")
                        expected_type = config.param_types[param_name]
                        params[param_name] = expected_type(params[param_name])
                    except Exception:
                        raise InvalidPolicyConfig(
                            f"'{config_key}' parameter '{param_name}' needs to be of type"
                            f" {config.param_types[param_name].__name__} (not {type(params[param_name]).__name__})"
                            f" for {class_name}!"
                        )

        # value isn't correct type
        if value is not None:
            if not isinstance(value, config.value_type):
                try:
                    # try casting to the right type
                    config.value_type(value)
                except Exception:
                    raise InvalidPolicyConfig(
                        f"'{config_key}' value needs to be of type"
                        f" {config.value_type.__name__} (not {type(value).__name__})"
                        f" for {class_name}!"
                    )

        return config

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
        return QuotaAllowance(
            can_run=True, max_threads=self.max_threads, explanation={}
        )

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        result_or_error: QueryResultOrError,
    ) -> None:
        pass


DEFAULT_PASSTHROUGH_POLICY = PassthroughPolicy(
    StorageKey("default.no_storage_key"),
    required_tenant_types=[],
    default_config_overrides={},
)

import_submodules_in_directory(
    os.path.dirname(os.path.realpath(__file__)), "snuba.query.allocation_policies"
)
