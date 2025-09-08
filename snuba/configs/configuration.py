import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, replace
from typing import Any, Type, TypedDict, TypeVar, cast, final

from snuba.datasets.storages.storage_key import StorageKey
from snuba.state import delete_config as delete_runtime_config
from snuba.state import get_all_configs as get_all_runtime_configs
from snuba.state import get_config as get_runtime_config
from snuba.state import set_config as set_runtime_config
from snuba.utils.registered_class import RegisteredClass

logger = logging.getLogger("snuba.configurable_component")

T = TypeVar("T", bound="ConfigurableComponent")


class InvalidConfig(Exception):
    pass


class ConfigurableComponentData(TypedDict):
    configurable_component_namespace: str
    configurable_component_config_key: str
    resource_identifier: str
    configurations: list[dict[str, Any]]
    optional_config_definitions: list[dict[str, Any]]


class ResourceIdentifier:
    def __init__(
        self,
        resource_identifier_name: StorageKey | str,
    ):
        self._resource_identifier_name = (
            resource_identifier_name.value
            if isinstance(resource_identifier_name, StorageKey)
            else resource_identifier_name
        )

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, ResourceIdentifier) and other.value == self.value

    def __repr__(self) -> str:
        return self.value

    @property
    def value(self) -> str:
        return self._resource_identifier_name


@dataclass()
class Configuration:
    """
    A configuration is a key-value pair that can be used to configure a ConfigurableComponent.
    example: configure whether an allocation policy is enforced or not via `is_enforced`.
    """

    name: str
    description: str
    value_type: type
    default: Any
    param_types: dict[str, type] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if type(self.default) is not self.value_type:
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

    def to_config_dict(self, value: Any = None, params: dict[str, Any] = {}) -> dict[str, Any]:
        """Returns a dict representation of a live Config."""
        return {
            **self.__to_base_dict(),
            "value": value if value is not None else self.default,
            "params": params,
        }


class ConfigurableComponent(ABC, metaclass=RegisteredClass):
    """
    A ConfigurableComponent is a component that can be configured via configurations.
    example: an allocation policy, a routing strategy, a strategy selector.

    Configurations are used to configure the component at runtime.
    example: configure whether an allocation policy is enforced or not via `is_enforced`.

    To make your own ConfigurableComponent:
    ===================================

        >>> class MyConfigurableComponent(ConfigurableComponent):

        >>>     def _additional_config_definitions(self) -> list[Configuration]:
        >>>         # Define component specific config definitions, these will be used along
        >>>         # with the default definitions of the base class. (is_enforced, is_active)
        >>>         pass

        >>>     def component_namespace(self) -> str:
        >>>         return "MyConfigurableComponent"

        >>>     def _get_default_config_definitions(self) -> list[Configuration]:
        >>>         return [
        >>>             Configuration(name="my_config", description="My config", value_type=int, default=100),
        >>>         ]

        >>>     def additional_config_definitions(self) -> list[Configuration]:
        >>>         # Define component specific config definitions, these will be used along
        >>>         # with the default definitions of the base class. (is_enforced, is_active)
        >>>         pass

        >>>     def resource_identifier(self) -> ResourceIdentifier:
        >>>         # describes what is the underlying resource of this ConfigurableComponent
        >>>         # example: BytesScannedRejectingPolicy (ConfigurableComponent) is a policy that is applied to a storage (underlying resource)
        >>>         return ResourceIdentifier(storage_key=StorageKey("my_storage"))

    """

    # This component builds redis strings that are delimited by dots, commas, colons
    # in order to allow those characters to exist in config we replace them with their
    # counterparts on write/read. It may be better to just replace our serialization with JSON
    # instead of what we're doing but this is where we're at rn 1/10/24
    _KEY_DELIMITERS_TO_ESCAPE_SEQUENCES = {
        ".": "__dot_literal__",
        ",": "__comma_literal__",
        ":": "__colon_literal__",
    }

    def component_name(self) -> str:
        # what is this configurable component's class name?
        # bytes scanned policy? outcomes based routing strategy?
        # needs to uniquely identify the configurable component
        return f"{self.resource_identifier.value}.{self.__class__.__name__}"

    @classmethod
    def component_namespace(cls) -> str:
        # is it an allocation policy? a routing strategy? a strategy selector?
        raise NotImplementedError

    @abstractmethod
    def _get_default_config_definitions(self) -> list[Configuration]:
        raise NotImplementedError

    @abstractmethod
    def additional_config_definitions(self) -> list[Configuration]:
        raise NotImplementedError

    @property
    @abstractmethod
    def resource_identifier(self) -> ResourceIdentifier:
        raise NotImplementedError

    def config_definitions(self) -> dict[str, Configuration]:
        """Returns a dictionary of configuration definitions on this ConfigurableComponent."""
        return {
            config.name: config
            for config in self._get_default_config_definitions()
            + self.additional_config_definitions()
        }

    def __unescape_delimiter_chars(self, key: str) -> str:
        for (
            delimiter_char,
            escape_sequence,
        ) in self._KEY_DELIMITERS_TO_ESCAPE_SEQUENCES.items():
            key = key.replace(escape_sequence, delimiter_char)
        return key

    @final
    def _validate_config_params(
        self, config_key: str, params: dict[str, Any], value: Any = None
    ) -> Configuration:
        definitions = self.config_definitions()

        class_name = self.__class__.__name__

        # config doesn't exist
        if config_key not in definitions:
            raise InvalidConfig(f"'{config_key}' is not a valid config for {class_name}!")

        config = definitions[config_key]

        # missing required parameters
        if (
            diff := {
                key: config.param_types[key].__name__
                for key in config.param_types
                if key not in params
            }
        ) != dict():
            raise InvalidConfig(
                f"'{config_key}' missing required parameters: {diff} for {class_name}!"
            )

        # not an optional config (no parameters)
        if params and not config.param_types:
            raise InvalidConfig(f"'{config_key}' takes no params for {class_name}!")

        # parameters aren't correct types
        if params:
            for param_name in params:
                if not isinstance(params[param_name], config.param_types[param_name]):
                    try:
                        # try casting to the right type, eg try int("10")
                        expected_type = config.param_types[param_name]
                        params[param_name] = expected_type(params[param_name])
                    except Exception:
                        raise InvalidConfig(
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
                    raise InvalidConfig(
                        f"'{config_key}' value needs to be of type"
                        f" {config.value_type.__name__} (not {type(value).__name__})"
                        f" for {class_name}!"
                    )

        return config

    def __deserialize_runtime_config_key(self, key: str) -> tuple[str, dict[str, Any]]:
        """
        Given a raw runtime config key, deconstructs it into it's config
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
                param_key = self.__unescape_delimiter_chars(param_key)
                param_value = self.__unescape_delimiter_chars(param_value)
                params_dict[param_key] = param_value

        self._validate_config_params(config_key=config_key, params=params_dict)

        return config_key, params_dict

    def get_current_configs(self) -> list[dict[str, Any]]:
        """Returns a list of live configs with their definitions on this ConfigurableComponent."""

        runtime_configs = get_all_runtime_configs(self._get_hash())
        definitions = self.config_definitions()

        required_configs = set(
            config_name
            for config_name, config_def in definitions.items()
            if not config_def.param_types
        )

        detailed_configs: list[dict[str, Any]] = []

        for key in runtime_configs:
            if key.startswith(self.component_name()):
                try:
                    config_key, params = self.__deserialize_runtime_config_key(key)
                except Exception:
                    logger.exception(
                        f"{self.component_namespace()} could not deserialize a key: {key}"
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

    def get_optional_config_definitions_json(self) -> list[dict[str, Any]]:
        """Returns a json-like dictionary of optional config definitions on this ConfigurableComponent."""
        return [
            definition.to_definition_dict()
            for definition in self.config_definitions().values()
            if definition.param_types
        ]

    @abstractmethod
    def _additional_config_definitions(self) -> list[Configuration]:
        """
        Define configurable component specific config definitions, these will be used along
        with the default definitions of the base class. (is_enforced, is_active)
        """
        pass

    def _get_overridden_additional_config_defaults(
        self, default_config_overrides: dict[str, Any]
    ) -> list[Configuration]:
        """overrides the defaults specified for the config in code with the default specified
        to the instance of the configurable component
        """
        definitions = self._additional_config_definitions()
        return [
            replace(
                definition,
                default=default_config_overrides.get(definition.name, definition.default),
            )
            for definition in definitions
        ]

    def __escape_delimiter_chars(self, key: str) -> str:
        if not isinstance(key, str):
            return key
        for (
            delimiter_char,
            escape_sequence,
        ) in self._KEY_DELIMITERS_TO_ESCAPE_SEQUENCES.items():
            if escape_sequence in str(key):
                raise InvalidConfig(f"{escape_sequence} is not a valid string for a policy config")
            key = key.replace(delimiter_char, escape_sequence)
        return key

    def __build_runtime_config_key(self, config: str, params: dict[str, Any]) -> str:
        """
        Builds a unique key to be used in the actual datastore containing these configs.

        Example return values:
        - `"mystorage.MyAllocationPolicy.my_config"`            # no params
        - `"mystorage.MyAllocationPolicy.my_config.a:1,b:2"`    # sorted params
        """
        parameters = "."
        for param in sorted(list(params.keys())):
            param_sanitized = self.__escape_delimiter_chars(param)
            value_sanitized = self.__escape_delimiter_chars(params[param])
            parameters += f"{param_sanitized}:{value_sanitized},"
        parameters = parameters[:-1]
        return f"{self.component_name()}.{config}{parameters}"

    def _get_hash(self) -> str:
        return self.component_namespace()

    def get_config_value(
        self,
        config_key: str,
        params: dict[str, Any] = {},
        validate: bool = True,
    ) -> Any:
        """Returns value of a configuration on this ConfigurableComponent, or the default if none exists in Redis."""
        config_definition = (
            self._validate_config_params(config_key, params)
            if validate
            else self.config_definitions()[config_key]
        )
        return get_runtime_config(
            key=self.__build_runtime_config_key(config_key, params),
            default=config_definition.default,
            config_key=self._get_hash(),
        )

    def set_config_value(
        self,
        config_key: str,
        value: Any,
        params: dict[str, Any] = {},
        user: str | None = None,
    ) -> None:
        """Sets a value of a configuration on this ConfigurableComponent."""
        config_definition = self._validate_config_params(config_key, params, value)
        # ensure correct type is stored
        value = config_definition.value_type(value)
        set_runtime_config(
            key=self.__build_runtime_config_key(config_key, params),
            value=value,
            user=user,
            force=True,
            config_key=self._get_hash(),
        )

    def delete_config_value(
        self,
        config_key: str,
        params: dict[str, Any] = {},
        user: str | None = None,
    ) -> None:
        """
        Deletes an instance of an optional configuration on this ConfigurableComponent.
        If this function is run on a required configuration, it resets the value to default instead.
        """
        self._validate_config_params(config_key, params)
        delete_runtime_config(
            key=self.__build_runtime_config_key(config_key, params),
            user=user,
            config_key=self._get_hash(),
        )

    @classmethod
    def config_key(cls) -> str:
        return f"{cls.component_namespace()}.{cls.__name__}"

    @classmethod
    def class_name(cls) -> str:
        return cls.__name__

    def to_dict(self) -> ConfigurableComponentData:
        return ConfigurableComponentData(
            configurable_component_namespace=self.component_namespace(),
            configurable_component_config_key=self.class_name(),
            resource_identifier=self.resource_identifier.value,
            configurations=self.get_current_configs(),
            optional_config_definitions=self.get_optional_config_definitions_json(),
        )

    @classmethod
    def get_component_class(cls, namespace: str) -> Type["ConfigurableComponent"]:
        return cast(
            Type["ConfigurableComponent"],
            cls.class_from_name(f"{namespace}.{namespace}"),
        )

    @classmethod
    def get_from_name(cls: Type[T], name: str) -> Type[T]:
        return cast(Type[T], cls.class_from_name(f"{cls.component_namespace()}.{name}"))
