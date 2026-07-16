from typing import cast

import pytest

from snuba.configs.configuration import (
    ConfigurableComponent,
    Configuration,
    InvalidConfig,
    ResourceIdentifier,
)
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import AllocationPolicy
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    RoutingStrategyConfig,
)
from tests.configs.component_config import (
    override_component_config,
    override_component_configs,
)


class SomeConfigurableComponent(ConfigurableComponent):
    def __init__(self) -> None:
        self._default_config_definitions = [
            RoutingStrategyConfig(
                name="default_config_1",
                description="A default configuration",
                value_type=int,
                default=100,
            ),
            RoutingStrategyConfig(
                name="override_config_for_org_id",
                description="override config for org",
                value_type=int,
                default=-1,
            ),
        ]
        self._overridden_additional_config_definitions = (
            self._get_overridden_additional_config_defaults({"additional_config_1": 50})
        )

    @classmethod
    def component_namespace(cls) -> str:
        return "SomeConfigurableComponent"

    def _get_default_config_definitions(self) -> list[Configuration]:
        return cast(list[Configuration], self._default_config_definitions)

    def additional_config_definitions(self) -> list[Configuration]:
        return self._overridden_additional_config_definitions

    def _additional_config_definitions(self) -> list[Configuration]:
        return [
            Configuration(
                name="additional_config_1",
                description="An additional configuration",
                value_type=int,
                default=100,
            ),
        ]

    @property
    def resource_identifier(self) -> ResourceIdentifier:
        return ResourceIdentifier("some_non_storage_resource")


class TestConfigurableComponentInvalid(SomeConfigurableComponent):
    """Test implementation that raises NotImplementedError for abstract methods."""

    @classmethod
    def component_namespace(cls) -> str:
        return "InvalidComponent"

    def _get_default_config_definitions(self) -> list[Configuration]:
        return []

    def additional_config_definitions(self) -> list[Configuration]:
        return []

    def _additional_config_definitions(self) -> list[Configuration]:
        return []

    @property
    def resource_identifier(self) -> ResourceIdentifier:
        return ResourceIdentifier(StorageKey("invalid_storage"))


class TestConfigurableComponentInvalidDefault(SomeConfigurableComponent):
    """Test implementation with invalid default value type."""

    def __init__(self) -> None:
        super().__init__()
        self._default_config_definitions = [
            RoutingStrategyConfig(
                name="invalid_config",
                description="A config with invalid default type",
                value_type=int,
                default="not_an_int",  # This should raise ValueError
            ),
        ]

    def _additional_config_definitions(self) -> list[Configuration]:
        return []


@pytest.fixture
def test_component() -> SomeConfigurableComponent:
    return SomeConfigurableComponent()


@pytest.mark.redis_db
class TestConfigurableComponentBasic:
    """Test basic functionality of ConfigurableComponent."""

    def test_component_name(self, test_component: SomeConfigurableComponent) -> None:
        assert (
            test_component.component_name() == "some_non_storage_resource.SomeConfigurableComponent"
        )

    def test_config_definitions(self, test_component: SomeConfigurableComponent) -> None:
        """Test that config_definitions returns all configurations."""
        assert {"default_config_1", "additional_config_1", "override_config_for_org_id"} == set(
            test_component.config_definitions().keys()
        )

    def test_get_current_configs(self, test_component: SomeConfigurableComponent) -> None:
        """get_current_configs returns one entry per config with its effective
        global value."""
        configs = test_component.get_current_configs()
        assert len(configs) == 3
        assert {
            "name": "default_config_1",
            "type": "int",
            "default": 100,
            "description": "A default configuration",
            "value": 100,
            "params": {},
        } in configs
        assert {
            "name": "additional_config_1",
            "type": "int",
            "default": 50,
            "description": "An additional configuration",
            "value": 50,
            "params": {},
        } in configs

        # a global override is reflected in the effective value
        with override_component_config(test_component, "default_config_1", 7):
            assert {
                "name": "default_config_1",
                "type": "int",
                "default": 100,
                "description": "A default configuration",
                "value": 7,
                "params": {},
            } in test_component.get_current_configs()

    def test_invalid_default_value_type(self) -> None:
        """Test that Configuration with invalid default value type raises ValueError."""
        with pytest.raises(
            ValueError,
            match="Config item `invalid_config` expects type <class 'int'> got value `not_an_int` of type <class 'str'>",
        ):
            TestConfigurableComponentInvalidDefault()


class TestConfigurableComponentValidation:
    """Test configuration validation."""

    def test_validate_config_valid(self, test_component: SomeConfigurableComponent) -> None:
        config = test_component._validate_config(config_key="override_config_for_org_id")
        assert config.name == "override_config_for_org_id"

    def test_validate_config_invalid_config(
        self, test_component: SomeConfigurableComponent
    ) -> None:
        """Test validation with invalid config key."""
        with pytest.raises(
            InvalidConfig,
            match="'invalid_config' is not a valid config for SomeConfigurableComponent",
        ):
            test_component._validate_config("invalid_config")

    def test_validate_config_wrong_value_type(
        self, test_component: SomeConfigurableComponent
    ) -> None:
        with pytest.raises(InvalidConfig, match="value needs to be of type int"):
            test_component._validate_config("override_config_for_org_id", value="not_an_int")


@pytest.mark.redis_db
class TestConfigurableComponentConfigOperations:
    """Test config get/set/delete operations."""

    def test_get_config_value_default(self, test_component: SomeConfigurableComponent) -> None:
        """Test getting config value with default."""
        assert test_component.get_config_value("default_config_1") == 100  # Default value

    def test_get_config_value_scoped(self, test_component: SomeConfigurableComponent) -> None:
        """Test getting a per-org scoped config value from a sentry-option override."""
        with override_component_config(
            test_component, "override_config_for_org_id", 100, {"organization_id": 10}
        ):
            assert (
                test_component.get_config_value(
                    "override_config_for_org_id", {"organization_id": 10}
                )
                == 100
            )
            # a different org falls back to the code default
            assert (
                test_component.get_config_value(
                    "override_config_for_org_id", {"organization_id": 99}
                )
                == -1
            )

    def test_get_config_value_override(self, test_component: SomeConfigurableComponent) -> None:
        """A sentry-option override takes precedence over the code default."""
        assert test_component.get_config_value("default_config_1") == 100
        with override_component_config(test_component, "default_config_1", 200):
            assert test_component.get_config_value("default_config_1") == 200

    def test_set_config_value_writes_to_redis(
        self, test_component: SomeConfigurableComponent
    ) -> None:
        """set/delete_config_value still write the legacy Redis store used by
        snuba-admin. The effect is observed via get_current_configs (not
        get_config_value, which now reads sentry-options only)."""
        config_key = "default_config_1"

        test_component.set_config_value(config_key=config_key, value=5)
        assert {
            "name": config_key,
            "type": "int",
            "default": 100,
            "description": "A default configuration",
            "value": 5,
            "params": {},
        } in test_component.get_current_configs()

        # default_config_1 is a required config, so deleting resets it to its default.
        test_component.delete_config_value(config_key=config_key)
        assert {
            "name": config_key,
            "type": "int",
            "default": 100,
            "description": "A default configuration",
            "value": 100,
            "params": {},
        } in test_component.get_current_configs()

    def test_delete_config_value_scoped(self, test_component: SomeConfigurableComponent) -> None:
        config_key = "override_config_for_org_id"
        tenant = {"organization_id": 10}

        test_component.set_config_value(config_key=config_key, value=100, tenant_ids=tenant)
        assert test_component.get_config_value(config_key, tenant) == 100

        # deleting the scoped override falls back to the code default
        test_component.delete_config_value(config_key=config_key, tenant_ids=tenant)
        assert test_component.get_config_value(config_key, tenant) == -1

    def test_get_config_value_invalid_config(
        self, test_component: SomeConfigurableComponent
    ) -> None:
        with pytest.raises(InvalidConfig):
            test_component.get_config_value("invalid_config")

    def test_set_config_value_invalid_config(
        self, test_component: SomeConfigurableComponent
    ) -> None:
        with pytest.raises(InvalidConfig):
            test_component.set_config_value("invalid_config", "value")

    def test_delete_config_value_invalid_config(
        self, test_component: SomeConfigurableComponent
    ) -> None:
        with pytest.raises(InvalidConfig):
            test_component.delete_config_value("invalid_config")


class TestConfigurableComponentConfigRetrieval:
    """Test config retrieval methods."""

    def test_get_optional_config_definitions_json(
        self, test_component: SomeConfigurableComponent
    ) -> None:
        definitions = test_component.get_optional_config_definitions_json()
        names = {d["name"] for d in definitions}
        # every config is scopable now (no declared params), so it's listed
        assert "override_config_for_org_id" in names
        assert "default_config_1" in names


class TestConfigurableComponentNamespaces:
    """Test namespace filtering in all_names() method."""

    def test_configurable_component_all_names_returns_all_classes(self) -> None:
        """Test that ConfigurableComponent.all_names() returns all registered classes from different namespaces."""
        namespaces = set()
        for name in ConfigurableComponent.all_names():
            namespace = name.split(".")[0]
            namespaces.add(namespace)

        # Should have multiple namespaces because as of 9/9/25, we have 2 namespaces: BaseRoutingStrategy and AllocationPolicy
        assert len(namespaces) > 1

    def test_subclass_all_names_filters_by_namespace(self) -> None:
        """Test that any subclass all_names() returns only classes in its namespace."""
        for name in AllocationPolicy.all_names():
            # ensures that the class name is valid within the AllocationPolicy namespace
            AllocationPolicy.get_from_name(name)
            # Should not include the base class itself
            assert (
                name
                != f"{AllocationPolicy.component_namespace()}.{AllocationPolicy.component_namespace()}"
            )


class TestConfigurableComponentScopedConfig:
    """Configs are scopable per id/referrer, resolved most-specific-first."""

    def test_scoped_precedence(self) -> None:
        component = SomeConfigurableComponent()
        key = "override_config_for_org_id"
        with override_component_configs(
            (component, key, 100, {"organization_id": 123, "referrer": "api.foo"}),
            (component, key, 500, {"organization_id": 123}),
            (component, key, 2000, {"referrer": "api.foo"}),
            (component, key, 9000),
        ):
            # (org, referrer) wins
            assert (
                component.get_config_value(key, {"organization_id": 123, "referrer": "api.foo"})
                == 100
            )
            # (org, "*")
            assert component.get_config_value(key, {"organization_id": 123, "referrer": "x"}) == 500
            # ("*", referrer)
            assert (
                component.get_config_value(key, {"organization_id": 9, "referrer": "api.foo"})
                == 2000
            )
            # ("*", "*") global
            assert component.get_config_value(key, {"organization_id": 9, "referrer": "x"}) == 9000
