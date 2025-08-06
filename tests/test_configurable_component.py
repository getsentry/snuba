from typing import Any, Dict
from unittest.mock import MagicMock, Mock, patch

import pytest

from snuba.configs.configuration import (
    ConfigurableComponent,
    Configuration,
    InvalidConfig,
    ResourceIdentifier,
)
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    RoutingStrategyConfig,
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
                param_types={"organization_id": int},
            ),
        ]
        self._overridden_additional_config_definitions = (
            self._get_overridden_additional_config_defaults({"additional_config_1": 50})
        )

    def component_namespace(self) -> str:
        return "SomeConfigurableComponent"

    def _get_default_config_definitions(self) -> list[Configuration]:
        return self._default_config_definitions

    def additional_config_definitions(self) -> list[Configuration]:
        return self._overridden_additional_config_definitions

    def _additional_config_definitions(self) -> list[Configuration]:
        return [
            RoutingStrategyConfig(
                name="additional_config_1",
                description="An additional configuration",
                value_type=int,
                default=100,
            ),
        ]

    @property
    def resource_identifier(self) -> ResourceIdentifier:
        return ResourceIdentifier("some_non_storage_resource")


# class TestConfigurableComponentWithOverrides(SomeConfigurableComponent):
#     """Test implementation with default config overrides."""

#     def __init__(self):
#         super().__init__()
#         self._overridden_additional_config_definitions = (
#             self._get_overridden_additional_config_defaults({
#                 "additional_config": False,
#                 "test_config": 200,
#             })
#         )

#     def additional_config_definitions(self) -> list[Configuration]:
#         return self._overridden_additional_config_definitions


class TestConfigurableComponentWithDelimiters(SomeConfigurableComponent):
    """Test implementation that uses delimiter characters in config names."""

    def __init__(self) -> None:
        super().__init__()
        self._default_config_definitions = [
            RoutingStrategyConfig(
                name="config.with.dots",
                description="A config with dots",
                value_type=str,
                default="default",
                param_types={"param:with:colons": str, "param,with,commas": int},
            ),
        ]

    def _additional_config_definitions(self) -> list[Configuration]:
        return []

    @property
    def resource_identifier(self) -> ResourceIdentifier:
        return ResourceIdentifier(StorageKey("test.storage"))


class TestConfigurableComponentInvalid(SomeConfigurableComponent):
    """Test implementation that raises NotImplementedError for abstract methods."""

    def component_namespace(self) -> str:
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


# @pytest.fixture
# def test_component_with_overrides():
#     return TestConfigurableComponentWithOverrides()


@pytest.fixture
def test_component_with_delimiters() -> TestConfigurableComponentWithDelimiters:
    return TestConfigurableComponentWithDelimiters()

@pytest.mark.redis_db
class TestConfigurableComponentBasic:
    """Test basic functionality of ConfigurableComponent."""

    def test_component_name(self, test_component: SomeConfigurableComponent) -> None:
        assert (
            test_component.component_name()
            == "some_non_storage_resource.SomeConfigurableComponent"
        )

    # @pytest.mark.redis_db
    def test_config_definitions(
        self, test_component: SomeConfigurableComponent
    ) -> None:
        """Test that config_definitions returns all configurations."""
        assert set(
            ["default_config_1", "additional_config_1", "override_config_for_org_id"]
        ) == set(test_component.config_definitions().keys())

    def test_get_current_configs(self, test_component: SomeConfigurableComponent) -> None:
        """Test that get_current_configs returns the correct configs."""
        configs = test_component.get_current_configs()
        assert len(configs) == 2
        assert {'name': 'default_config_1', 'type': 'int', 'default': 100, 'description': 'A default configuration', 'value': 100, 'params': {}} in configs
        assert {'name': 'additional_config_1', 'type': 'int', 'default': 50, 'description': 'An additional configuration', 'value': 50, 'params': {}} in configs

        # add an instance of an optional config
        test_component.set_config_value(config_key="override_config_for_org_id", value=100, params={"organization_id": 10})
        configs = test_component.get_current_configs()
        assert len(configs) == 3
        assert {'name': 'default_config_1', 'type': 'int', 'default': 100, 'description': 'A default configuration', 'value': 100, 'params': {}} in configs
        assert {'name': 'additional_config_1', 'type': 'int', 'default': 50, 'description': 'An additional configuration', 'value': 50, 'params': {}} in configs
        assert {
            "name": "override_config_for_org_id",
            "type": "int",
            "default": -1,
            "description": "override config for org",
            "params": {"organization_id": 10},
            "value": 100,
        } in configs

    def test_invalid_default_value_type(self) -> None:
        """Test that Configuration with invalid default value type raises ValueError."""
        with pytest.raises(
            ValueError, match="Config item `invalid_config` expects type <class 'int'> got value `not_an_int` of type <class 'str'>"
        ):
            TestConfigurableComponentInvalidDefault()

    # do set config delete config get config


class TestConfigurableComponentValidation:
    """Test configuration parameter validation."""

    def test_validate_config_params_valid_config(self, test_component: SomeConfigurableComponent) -> None:
        """Test validation with valid config and parameters."""
        config = test_component._validate_config_params(
            config_key="override_config_for_org_id",
            params={"organization_id": 42}
        )
        assert config.name == "override_config_for_org_id"
        assert config.param_types == {"organization_id": int}

    def test_validate_config_params_invalid_config(self, test_component: SomeConfigurableComponent) -> None:
        """Test validation with invalid config key."""
        with pytest.raises(InvalidConfig, match="'invalid_config' is not a valid config for SomeConfigurableComponent"):
            test_component._validate_config_params("invalid_config", {})

    def test_validate_config_params_missing_required_params(self, test_component: SomeConfigurableComponent) -> None:
        with pytest.raises(InvalidConfig, match="'override_config_for_org_id' missing required parameters: {'organization_id': 'int'} for SomeConfigurableComponent!"):
            test_component._validate_config_params("override_config_for_org_id", {})

    def test_validate_config_params_wrong_param_type(self, test_component: SomeConfigurableComponent) -> None:
        with pytest.raises(InvalidConfig, match="parameter 'organization_id' needs to be of type int"):
            test_component._validate_config_params(
                "override_config_for_org_id",
                {"organization_id": "not_an_int"}
            )

    def test_validate_config_params_wrong_value_type(self, test_component: SomeConfigurableComponent) -> None:
        with pytest.raises(InvalidConfig, match="value needs to be of type int"):
            test_component._validate_config_params(
                "override_config_for_org_id",
                {"organization_id": 42},
                value="not_an_int"
            )


@pytest.mark.redis_db
class TestConfigurableComponentConfigOperations:
    """Test config get/set/delete operations."""

    def test_get_config_value_default(self, test_component: SomeConfigurableComponent) -> None:
        """Test getting config value with default."""
        assert test_component.get_config_value("default_config_1") == 100  # Default value

    def test_get_config_value_with_params(self, test_component: SomeConfigurableComponent) -> None:
        """Test getting config value with parameters."""
        test_component.set_config_value("override_config_for_org_id", 100, params={"organization_id": 10})

        assert test_component.get_config_value(
            "override_config_for_org_id",
            params={"organization_id": 10}
        ) == 100

    def test_set_config_value(self, test_component: SomeConfigurableComponent) -> None:
        """Test setting config value."""
        test_component.set_config_value("default_config_1", 200)
        assert test_component.get_config_value("default_config_1") == 200

    def test_delete_config_value(self, test_component: SomeConfigurableComponent) -> None:
        """Test deleting config value."""
        config_key = "default_config_1"

        test_component.set_config_value(config_key=config_key, value=5)
        assert test_component.get_config_value(config_key=config_key) == 5

        test_component.delete_config_value(config_key=config_key)
        # back to default
        assert test_component.get_config_value(config_key=config_key) == 100

    def test_delete_config_value_with_params(self, test_component: SomeConfigurableComponent) -> None:
        config_key = "override_config_for_org_id"
        params = {"organization_id": 10}

        test_component.set_config_value(config_key=config_key, value=100, params=params)
        assert test_component.get_config_value(config_key=config_key, params=params) == 100

        test_component.delete_config_value(config_key=config_key, params=params)
        # back to default
        assert test_component.get_config_value(config_key=config_key, params=params) == -1

    def test_get_config_value_invalid_config(self, test_component: SomeConfigurableComponent) -> None:
        with pytest.raises(InvalidConfig):
            test_component.get_config_value("invalid_config")

    def test_set_config_value_invalid_config(self, test_component: SomeConfigurableComponent) -> None:
        with pytest.raises(InvalidConfig):
            test_component.set_config_value("invalid_config", "value")

    def test_delete_config_value_invalid_config(self, test_component: SomeConfigurableComponent) -> None:
        with pytest.raises(InvalidConfig):
            test_component.delete_config_value("invalid_config")


class TestConfigurableComponentConfigRetrieval:
    """Test config retrieval methods."""

    def test_get_optional_config_definitions_json(self, test_component: SomeConfigurableComponent) -> None:
        optional_configs = test_component.get_optional_config_definitions_json()

        assert len(optional_configs) == 1
        assert optional_configs[0]["name"] == "override_config_for_org_id"
        assert "params" in optional_configs[0]
        assert len(optional_configs[0]["params"]) == 1
        assert optional_configs[0]["params"][0]["name"] == "organization_id"
        assert optional_configs[0]["params"][0]["type"] == "int"

