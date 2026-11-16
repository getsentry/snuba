import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, TypeVar, cast, final

from snuba.datasets.storages.storage_key import StorageKey
from snuba.state.sentry_options import get_mapped_option
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
# ConfigurableComponents (the storage-routing strategies) read this option in
# ``get_config_value``, falling back only to the code default.
CONFIGURABLE_COMPONENT_OVERRIDES_KEY = "configurable_component_overrides"

# A parameterized config key is ``{resource}.{ClassName}.{config}`` followed by a
# sorted, PARAM_DELIMITER-joined run of ``{name}PARAM_KV_DELIMITER{value}`` pairs,
# e.g. ``errors.MyPolicy.my_config|organization_id:1|referrer:api.foo``.
PARAM_DELIMITER = "|"
PARAM_KV_DELIMITER = ":"


class InvalidConfig(Exception):
    pass


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


class ConfigurableComponent(ABC, metaclass=RegisteredClass):
    """
    A ConfigurableComponent is a component that can be configured via configurations.
    example: a routing strategy.

    Configurations are used to configure the component at runtime.

    To make your own ConfigurableComponent:
    ===================================

        >>> class MyConfigurableComponent(ConfigurableComponent):

        >>>     def _additional_config_definitions(self) -> list[Configuration]:
        >>>         # Define component specific config definitions, these will be used along
        >>>         # with the default definitions of the base class.
        >>>         pass

        >>>     def component_namespace(self) -> str:
        >>>         return "MyConfigurableComponent"

        >>>     def _get_default_config_definitions(self) -> list[Configuration]:
        >>>         return [
        >>>             Configuration(name="my_config", description="My config", value_type=int, default=100),
        >>>         ]

        >>>     def resource_identifier(self) -> ResourceIdentifier:
        >>>         # describes what is the underlying resource of this ConfigurableComponent
        >>>         # example: BytesScannedRejectingPolicy (ConfigurableComponent) is a policy that is applied to a storage (underlying resource)
        >>>         return ResourceIdentifier(storage_key=StorageKey("my_storage"))

    """

    def component_name(self) -> str:
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

    @property
    @abstractmethod
    def resource_identifier(self) -> ResourceIdentifier:
        raise NotImplementedError

    def config_definitions(self) -> dict[str, Configuration]:
        """Returns a dictionary of configuration definitions on this ConfigurableComponent."""
        return {
            config.name: config
            for config in self._get_default_config_definitions()
            + self._additional_config_definitions()
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

    @abstractmethod
    def _additional_config_definitions(self) -> list[Configuration]:
        """
        Define configurable component specific config definitions, these will be used along
        with the default definitions of the base class.
        """
        pass

    def _build_config_key(self, config: str, params: dict[str, Any]) -> str:
        """Builds the fully-qualified config key used to look up an override.

        - no params -> `"mystorage.MyAllocationPolicy.my_config"`
        - sorted params -> `"mystorage.MyAllocationPolicy.my_config|a:1|b:2"`
        """
        key = f"{self.component_name()}.{config}"
        for param in sorted(params):
            key += f"{PARAM_DELIMITER}{param}{PARAM_KV_DELIMITER}{params[param]}"
        return key

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
        # Casting the code default is a no-op (Configuration.__post_init__ enforces
        # ``type(default) is value_type``), so no separate "missing" branch is needed.
        raw = get_mapped_option(
            CONFIGURABLE_COMPONENT_OVERRIDES_KEY,
            self._build_config_key(config_key, params),
            config_definition.default,
        )
        return config_definition.value_type(raw)

    @classmethod
    def config_key(cls) -> str:
        return f"{cls.component_namespace()}.{cls.__name__}"

    @classmethod
    def class_name(cls) -> str:
        return cls.__name__

    @classmethod
    def get_from_name(cls: type[T], name: str) -> type[T]:
        return cast(type[T], cls.class_from_name(f"{cls.component_namespace()}.{name}"))
