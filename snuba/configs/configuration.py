import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, replace
from typing import Any, TypedDict, TypeVar, cast, final

from sentry_options import OptionValue

from snuba.datasets.storages.storage_key import StorageKey
from snuba.state.sentry_options import get_option
from snuba.utils.registered_class import RegisteredClass

logger = logging.getLogger("snuba.configurable_component")

T = TypeVar("T", bound="ConfigurableComponent")

# Single sentry-options dict holding ConfigurableComponent config overrides,
# keyed by the fully-qualified config key
# ``{resource}.{ClassName}.{config}[|{param}:{value}|...]`` (params sorted,
# ``|``-delimited). Values are stored as numbers and cast to each config's
# declared numeric ``value_type`` (int/float) on read. This option is the
# authoritative, centrally-managed (sentry-options-automator) source; it is
# read-only at runtime.
#
# All ConfigurableComponents (allocation policies and storage-routing strategies)
# read this option in ``get_config_value``, falling back only to the code
# default. The policies themselves are unchanged -- only where the config value
# comes from has moved.
CONFIGURABLE_COMPONENT_OVERRIDES_KEY = "configurable_component_overrides"


class InvalidConfig(Exception):
    pass


class ConfigurableComponentData(TypedDict):
    configurable_component_namespace: str
    configurable_component_class_name: str
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

    def to_config_dict(
        self, value: Any = None, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Returns a dict representation of a live Config."""
        if params is None:
            params = {}
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

    def component_name(self) -> str:
        # what is this configurable component's class name?
        # bytes scanned policy? outcomes based routing strategy?
        # needs to uniquely identify the configurable component
        return f"{self.resource_identifier.value}.{self.__class__.__name__}"

    @classmethod
    def component_namespace(cls) -> str:
        """
        Let's say the inheritance chain is:
        ConfigurableComponent
            SomeBaseClass
                SomeConcreteClass
            SomeOtherBaseClass
                SomeOtherConcreteClass
                    SomeEvenMoreConcreteClass

        The namespace for SomeBaseClass should be "SomeBaseClass"
        The namespace for SomeConcreteClass should be "SomeBaseClass"
        The namespace for SomeOtherBaseClass should be "SomeOtherBaseClass"
        The namespace for SomeOtherConcreteClass should be "SomeOtherBaseClass"
        The namespace for SomeEvenMoreConcreteClass should be "SomeOtherBaseClass"
        """

        for base in cls.__mro__:
            if ConfigurableComponent in base.__bases__:
                return base.__name__
        raise RuntimeError(f"{cls.__name__} does not inherit from ConfigurableComponent")

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
        ) != {}:
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
                    except Exception as e:
                        raise InvalidConfig(
                            f"'{config_key}' parameter '{param_name}' needs to be of type"
                            f" {config.param_types[param_name].__name__} (not {type(params[param_name]).__name__})"
                            f" for {class_name}!"
                        ) from e

        # value isn't correct type
        if value is not None and not isinstance(value, config.value_type):
            try:
                # try casting to the right type
                config.value_type(value)
            except Exception as e:
                raise InvalidConfig(
                    f"'{config_key}' value needs to be of type"
                    f" {config.value_type.__name__} (not {type(value).__name__})"
                    f" for {class_name}!"
                ) from e

        return config

    def __deserialize_config_key(self, key: str) -> tuple[str, dict[str, Any]]:
        """
        Given a fully-qualified config key, deconstructs it into its config key
        and parameters components.

        Examples:
        - `"mystorage.MyAllocationPolicy.my_config"`
            - returns `"my_config", {}`
        - `"mystorage.MyAllocationPolicy.my_config|a:1|b:2"`
            - returns `"my_config", {"a": 1, "b": 2}`
        """
        # base is "storage.policy.config"; params (if any) follow after a "|",
        # each a "param:value" pair, "|"-delimited.
        base, _, params_string = key.partition("|")
        config_key = base[len(self.component_name()) + 1 :]
        params_dict = {}
        if params_string:
            for param_string in params_string.split("|"):
                param_key, _, param_value = param_string.partition(":")
                params_dict[param_key] = param_value

        self._validate_config_params(config_key=config_key, params=params_dict)

        return config_key, params_dict

    def get_current_configs(self) -> list[dict[str, Any]]:
        """Returns a list of live configs with their definitions on this ConfigurableComponent.

        Reads the centrally-managed ``configurable_component_overrides`` option.
        """
        overrides: OptionValue = get_option(CONFIGURABLE_COMPONENT_OVERRIDES_KEY, {})
        overrides_map: dict[str, Any] = overrides if isinstance(overrides, dict) else {}
        definitions = self.config_definitions()

        required_configs = {
            config_name
            for config_name, config_def in definitions.items()
            if not config_def.param_types
        }

        detailed_configs: list[dict[str, Any]] = []

        for key in overrides_map:
            if key.startswith(self.component_name()):
                try:
                    config_key, params = self.__deserialize_config_key(key)
                except Exception:
                    logger.exception(
                        f"{self.component_namespace()} could not deserialize a key: {key}"
                    )
                    continue
                detailed_configs.append(
                    definitions[config_key].to_config_dict(value=overrides_map[key], params=params)
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

    def _build_config_key(self, config: str, params: dict[str, Any]) -> str:
        """
        Builds the fully-qualified config key used to look up an override.

        Example return values:
        - `"mystorage.MyAllocationPolicy.my_config"`            # no params
        - `"mystorage.MyAllocationPolicy.my_config|a:1|b:2"`    # sorted params
        """
        base = f"{self.component_name()}.{config}"
        if not params:
            return base
        suffix = "|".join(f"{param}:{params[param]}" for param in sorted(params))
        return f"{base}|{suffix}"

    def get_config_value(
        self,
        config_key: str,
        params: dict[str, Any] | None = None,
        validate: bool = True,
    ) -> Any:
        """Returns the value of a configuration on this ConfigurableComponent.

        Reads from the centrally-managed ``configurable_component_overrides``
        sentry-option (values stored as numbers, cast to the config's declared
        int/float type), or the code default when no override is set."""
        if params is None:
            params = {}
        config_definition = (
            self._validate_config_params(config_key, params)
            if validate
            else self.config_definitions()[config_key]
        )
        full_key = self._build_config_key(config_key, params)
        overrides: OptionValue = get_option(CONFIGURABLE_COMPONENT_OVERRIDES_KEY, {})
        if isinstance(overrides, dict) and full_key in overrides:
            return config_definition.value_type(overrides[full_key])
        return config_definition.default

    @classmethod
    def config_key(cls) -> str:
        return f"{cls.component_namespace()}.{cls.__name__}"

    @classmethod
    def class_name(cls) -> str:
        return cls.__name__

    def to_dict(self) -> ConfigurableComponentData:
        return ConfigurableComponentData(
            configurable_component_namespace=self.component_namespace(),
            configurable_component_class_name=self.class_name(),
            resource_identifier=self.resource_identifier.value,
            configurations=self.get_current_configs(),
            optional_config_definitions=self.get_optional_config_definitions_json(),
        )

    @classmethod
    def get_component_class(cls, namespace: str) -> type["ConfigurableComponent"]:
        return cast(
            type["ConfigurableComponent"],
            cls.class_from_name(f"{namespace}.{namespace}"),
        )

    @classmethod
    def get_from_name(cls: type[T], name: str) -> type[T]:
        return cast(type[T], cls.class_from_name(f"{cls.component_namespace()}.{name}"))

    @classmethod
    def create_minimal_instance(cls, resource_identifier: str) -> "ConfigurableComponent":
        raise NotImplementedError

    @classmethod
    def all_names(cls) -> list[str]:
        """Returns all registered class names that belong to this component's namespace."""
        # If called on ConfigurableComponent itself, return all registered classes
        if cls is ConfigurableComponent:
            return list(cls._registry.all_names())

        # For subclasses, return only classes in the same namespace
        namespaced_classes = []
        for registered_cls in cls._registry.all_classes():
            if (
                hasattr(registered_cls, "component_namespace")
                and registered_cls.component_namespace() == cls.component_namespace()
                and registered_cls.config_key() != cls.config_key()
            ):
                namespaced_classes.append(
                    cast("ConfigurableComponent", registered_cls).class_name()
                )
        return namespaced_classes
