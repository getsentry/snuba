import logging
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any, TypedDict, TypeVar, cast, final

from sentry_options import OptionValue

from snuba import settings
from snuba.datasets.storages.storage_key import StorageKey
from snuba.state import delete_config as delete_runtime_config
from snuba.state import set_config as set_runtime_config
from snuba.state.sentry_options import SNUBA_OPTIONS_NAMESPACE, get_option
from snuba.utils.registered_class import RegisteredClass

logger = logging.getLogger("snuba.configurable_component")

T = TypeVar("T", bound="ConfigurableComponent")
V = TypeVar("V")

CONFIGURABLE_COMPONENT_OVERRIDES_KEY = "configurable_component_overrides"

SCOPED_OVERRIDE_WILDCARD = "*"


def resolve_scoped_override(
    scoped: Mapping[str, Any],
    scope_ids: int | str | Sequence[int | str | None] | None,
    referrer: str | None,
    default: V,
) -> V:
    """Resolve a config's nested override to a single value.

    ``scoped`` is one config's override entry, shaped as
    ``{id (or "*"): {referrer (or "*"): value}}`` where ``id`` is a
    project_id/organization_id. This lets one config carry a global value, a
    per-id value, a per-referrer value, or a per-(id, referrer) value at once.

    ``scope_ids`` is the id(s) a query is scoped by, **most-specific-first** --
    e.g. ``[project_id, organization_id]`` -- so a query carrying both a project
    and an org still matches an org-level override. A single id may be passed as
    a scalar. Lookups are most-specific-first, and the first match wins; for two
    ids the order is::

        (project, referrer) > (project, "*")
            > (org, referrer) > (org, "*")
            > ("*", referrer) > ("*", "*") > default

    ``("*", "*")`` is the global override tier (distinct from the code
    ``default``). Ids are stringified to match the JSON object keys. A ``None``
    id or referrer simply skips the lookups that would need it.
    """
    if scope_ids is None or isinstance(scope_ids, (str, int)):
        scope_ids = [scope_ids]
    w = SCOPED_OVERRIDE_WILDCARD
    id_keys = [str(s) for s in scope_ids if s is not None]
    inners = [referrer, w] if referrer is not None else [w]
    for outer in [*id_keys, w]:
        inner_map = scoped.get(outer)
        if not isinstance(inner_map, Mapping):
            continue
        for inner in inners:
            if inner in inner_map:
                return cast(V, inner_map[inner])
    return default


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
    def _validate_config(self, config_key: str, value: Any = None) -> Configuration:
        definitions = self.config_definitions()
        class_name = self.__class__.__name__

        # config doesn't exist
        if config_key not in definitions:
            raise InvalidConfig(f"'{config_key}' is not a valid config for {class_name}!")

        config = definitions[config_key]

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

    def get_current_configs(self) -> list[dict[str, Any]]:
        """Returns each config with its effective global value (the ``("*", "*")``
        override if set, else the code default)."""
        return [
            definition.to_config_dict(value=self.get_config_value(name))
            for name, definition in self.config_definitions().items()
        ]

    def get_optional_config_definitions_json(self) -> list[dict[str, Any]]:
        """Returns a json-like dictionary of optional config definitions on this ConfigurableComponent."""
        return [
            definition.to_definition_dict() for definition in self.config_definitions().values()
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

    def _build_config_key(self, config: str) -> str:
        """The key under which this config's overrides live: ``resource.Class.config``.

        Scoping (global / per-id / per-referrer / per-(id, referrer)) lives inside
        the value as a nested object, not in the key.
        """
        return f"{self.component_name()}.{config}"

    @staticmethod
    def _scope_ids(tenant_ids: dict[str, Any]) -> tuple[list[str | int], str | None]:
        """The (ids, referrer) a config is scoped by for a query. ``ids`` is
        ordered most-specific-first -- project_id then organization_id -- so a
        query carrying both still matches an org-level override."""
        ids: list[str | int] = [
            tenant_ids[key]
            for key in ("project_id", "organization_id")
            if tenant_ids.get(key) is not None
        ]
        referrer = tenant_ids.get("referrer")
        return ids, referrer

    def _get_hash(self) -> str:
        return self.component_namespace()

    def get_config_value(
        self,
        config_key: str,
        tenant_ids: dict[str, Any] | None = None,
        validate: bool = True,
    ) -> Any:
        """Returns the value of a configuration on this ConfigurableComponent.

        Reads from the centrally-managed ``configurable_component_overrides``
        sentry-option, which stores each config as a nested object
        ``{id (or "*"): {referrer (or "*"): value}}``. The value is resolved for
        the query's tenants most-specific-first, trying project_id then
        organization_id then the ``"*"`` wildcard on each axis, falling back to
        the code default. The legacy Redis runtime config is not consulted.
        """
        config_definition = (
            self._validate_config(config_key) if validate else self.config_definitions()[config_key]
        )
        overrides: OptionValue = get_option(CONFIGURABLE_COMPONENT_OVERRIDES_KEY, {})
        entry = (
            overrides.get(self._build_config_key(config_key))
            if isinstance(overrides, dict)
            else None
        )
        scoped: Mapping[str, Any] = entry if isinstance(entry, Mapping) else {}
        scope_ids, referrer = self._scope_ids(tenant_ids or {})
        value = resolve_scoped_override(scoped, scope_ids, referrer, config_definition.default)
        return config_definition.value_type(value)

    def set_config_value(
        self,
        config_key: str,
        value: Any,
        tenant_ids: dict[str, Any] | None = None,
        user: str | None = None,
    ) -> None:
        """Sets a config value, scoped to ``tenant_ids`` (empty = the global
        ``("*", "*")`` scope)."""
        config_definition = self._validate_config(config_key, value)
        value = config_definition.value_type(value)
        full_key = self._build_config_key(config_key)
        set_runtime_config(
            key=full_key,
            value=value,
            user=user,
            force=True,
            config_key=self._get_hash(),
        )
        if settings.TESTING:
            # Reads come from sentry-options, not Redis, so mirror the write into
            # the option store the test process reads back. This keeps
            # set_config_value the single config-setting API in tests (no bespoke
            # override helper); tests/conftest.py clears these between tests.
            self.__mirror_test_option_override(full_key, value, tenant_ids or {})

    def __mirror_test_option_override(
        self,
        full_key: str,
        value: Any,
        tenant_ids: dict[str, Any],
        *,
        remove: bool = False,
    ) -> None:
        from sentry_options._core import _set_override

        scope_ids, referrer = self._scope_ids(tenant_ids)
        # Writes land on the most-specific id present (project over org).
        outer = str(scope_ids[0]) if scope_ids else SCOPED_OVERRIDE_WILDCARD
        inner = referrer if referrer is not None else SCOPED_OVERRIDE_WILDCARD

        current: OptionValue = get_option(CONFIGURABLE_COMPONENT_OVERRIDES_KEY, {})
        merged: dict[str, Any] = dict(current) if isinstance(current, dict) else {}
        existing_entry = merged.get(full_key)
        entry: dict[str, Any] = dict(existing_entry) if isinstance(existing_entry, dict) else {}
        existing_inner = entry.get(outer)
        inner_map: dict[str, Any] = dict(existing_inner) if isinstance(existing_inner, dict) else {}
        if remove:
            inner_map.pop(inner, None)
        else:
            inner_map[inner] = value
        if inner_map:
            entry[outer] = inner_map
        else:
            entry.pop(outer, None)
        if entry:
            merged[full_key] = entry
        else:
            merged.pop(full_key, None)
        _set_override(SNUBA_OPTIONS_NAMESPACE, CONFIGURABLE_COMPONENT_OVERRIDES_KEY, merged)

    def delete_config_value(
        self,
        config_key: str,
        tenant_ids: dict[str, Any] | None = None,
        user: str | None = None,
    ) -> None:
        """Deletes a config's override for the given scope (empty ``tenant_ids`` =
        the global scope), falling back to the code default."""
        self._validate_config(config_key)
        full_key = self._build_config_key(config_key)
        delete_runtime_config(
            key=full_key,
            user=user,
            config_key=self._get_hash(),
        )
        if settings.TESTING:
            self.__mirror_test_option_override(full_key, None, tenant_ids or {}, remove=True)

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
