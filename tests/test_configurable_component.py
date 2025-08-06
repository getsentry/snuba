import pytest
from unittest.mock import Mock, patch, MagicMock
from typing import Any, Dict

from snuba.configs.configuration import (
    ConfigurableComponent,
    Configuration,
    ResourceIdentifier,
    InvalidConfig,
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
                name="default_config_2",
                description="Another default configuration",
                value_type=str,
                default="default_value",
                param_types={"param1": str, "param2": int},
            ),
        ]
        self._overridden_additional_config_definitions = (
            self._get_overridden_additional_config_defaults({"default_config_1": 50})
        )

    def component_namespace(self) -> str:
        return "SomeConfigurableComponent"

    def _get_default_config_definitions(self) -> list[Configuration]:
        return self._default_config_definitions

    def additional_config_definitions(self) -> list[Configuration]:
        return self._overridden_additional_config_definitions

    def _additional_config_definitions(self) -> list[Configuration]:
        return []

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


class TestConfigurableComponentBasic:
    """Test basic functionality of ConfigurableComponent."""

    def test_component_name(self, test_component: SomeConfigurableComponent) -> None:
        """Test that component_name returns the expected format."""
        assert test_component.component_name() == "some_non_storage_resource.SomeConfigurableComponent"

    def test_config_definitions(self, test_component: SomeConfigurableComponent) -> None:
        """Test that config_definitions returns all configurations."""
        definitions = test_component.config_definitions()

        # Should contain all configs from both default and additional
        assert "test_config" in definitions
        assert "optional_config" in definitions
        assert "additional_config" in definitions

        # Check that the configurations are properly merged
        assert len(definitions) == 3

        # Check that the configurations have the correct types
        assert isinstance(definitions["test_config"], Configuration)
        assert isinstance(definitions["optional_config"], Configuration)
        assert isinstance(definitions["additional_config"], Configuration)

    def test_abstract_methods_raise_not_implemented(self) -> None:
        """Test that abstract methods raise NotImplementedError when not implemented."""
        component = TestConfigurableComponentInvalid()

        # These should not raise NotImplementedError since they are implemented
        assert component.component_namespace() == "InvalidComponent"
        assert len(component._get_default_config_definitions()) == 0
        assert len(component.additional_config_definitions()) == 0

    def test_invalid_default_value_type(self) -> None:
        """Test that Configuration with invalid default value type raises ValueError."""
        with pytest.raises(
            ValueError, match="expects type int got value `not_an_int` of type str"
        ):
            TestConfigurableComponentInvalidDefault()


# class TestConfigurableComponentValidation:
#     """Test configuration parameter validation."""

#     def test_validate_config_params_valid_config(self, test_component):
#         """Test validation with valid config and parameters."""
#         config = test_component._validate_config_params(
#             config_key="optional_config",
#             params={"param1": "value1", "param2": 42}
#         )
#         assert config.name == "optional_config"
#         assert config.param_types == {"param1": str, "param2": int}

#     def test_validate_config_params_invalid_config(self, test_component):
#         """Test validation with invalid config key."""
#         with pytest.raises(InvalidConfig, match="'invalid_config' is not a valid config for TestConfigurableComponent"):
#             test_component._validate_config_params("invalid_config", {})

#     def test_validate_config_params_missing_required_params(self, test_component):
#         """Test validation with missing required parameters."""
#         with pytest.raises(InvalidConfig, match="'optional_config' missing required parameters"):
#             test_component._validate_config_params("optional_config", {"param1": "value1"})

#     def test_validate_config_params_extra_params_for_required_config(self, test_component):
#         """Test validation when providing params for a config that doesn't accept them."""
#         with pytest.raises(InvalidConfig, match="'test_config' takes no params for TestConfigurableComponent"):
#             test_component._validate_config_params("test_config", {"extra_param": "value"})

#     def test_validate_config_params_wrong_param_type(self, test_component):
#         """Test validation with wrong parameter type."""
#         with pytest.raises(InvalidConfig, match="parameter 'param2' needs to be of type int"):
#             test_component._validate_config_params(
#                 "optional_config",
#                 {"param1": "value1", "param2": "not_an_int"}
#             )

#     def test_validate_config_params_param_type_conversion(self, test_component):
#         """Test that parameter types are automatically converted when possible."""
#         config = test_component._validate_config_params(
#             "optional_config",
#             {"param1": "value1", "param2": "42"}  # String that can be converted to int
#         )
#         assert config.name == "optional_config"

#     def test_validate_config_params_wrong_value_type(self, test_component):
#         """Test validation with wrong value type."""
#         with pytest.raises(InvalidConfig, match="value needs to be of type int"):
#             test_component._validate_config_params(
#                 "test_config",
#                 {},
#                 value="not_an_int"
#             )

#     def test_validate_config_params_value_type_conversion(self, test_component):
#         """Test that value types are automatically converted when possible."""
#         config = test_component._validate_config_params(
#             "test_config",
#             {},
#             value="100"  # String that can be converted to int
#         )
#         assert config.name == "test_config"


# class TestConfigurableComponentDelimiters:
#     """Test delimiter escaping and unescaping functionality."""

#     def test_escape_delimiter_chars(self, test_component_with_delimiters):
#         """Test that delimiter characters are properly escaped."""
#         component = test_component_with_delimiters

#         # Test escaping of dots, commas, and colons
#         assert component._TestConfigurableComponent__escape_delimiter_chars("test.key") == "test__dot_literal__key"
#         assert component._TestConfigurableComponent__escape_delimiter_chars("test,key") == "test__comma_literal__key"
#         assert component._TestConfigurableComponent__escape_delimiter_chars("test:key") == "test__colon_literal__key"

#         # Test escaping of multiple delimiters
#         assert component._TestConfigurableComponent__escape_delimiter_chars("test.key,value:data") == "test__dot_literal__key__comma_literal__value__colon_literal__data"

#     def test_unescape_delimiter_chars(self, test_component_with_delimiters):
#         """Test that escaped delimiter characters are properly unescaped."""
#         component = test_component_with_delimiters

#         # Test unescaping of dots, commas, and colons
#         assert component._TestConfigurableComponent__unescape_delimiter_chars("test__dot_literal__key") == "test.key"
#         assert component._TestConfigurableComponent__unescape_delimiter_chars("test__comma_literal__key") == "test,key"
#         assert component._TestConfigurableComponent__unescape_delimiter_chars("test__colon_literal__key") == "test:key"

#         # Test unescaping of multiple delimiters
#         assert component._TestConfigurableComponent__unescape_delimiter_chars("test__dot_literal__key__comma_literal__value__colon_literal__data") == "test.key,value:data"

#     def test_escape_delimiter_chars_invalid_escape_sequence(self, test_component_with_delimiters):
#         """Test that using escape sequences in input raises InvalidConfig."""
#         component = test_component_with_delimiters

#         with pytest.raises(InvalidConfig, match="__dot_literal__ is not a valid string for a policy config"):
#             component._TestConfigurableComponent__escape_delimiter_chars("test__dot_literal__key")

#     def test_escape_delimiter_chars_non_string_input(self, test_component_with_delimiters):
#         """Test that non-string input is returned as-is."""
#         component = test_component_with_delimiters

#         assert component._TestConfigurableComponent__escape_delimiter_chars(42) == 42
#         assert component._TestConfigurableComponent__escape_delimiter_chars(None) is None


# class TestConfigurableComponentRuntimeConfig:
#     """Test runtime config key building and deserialization."""

#     def test_build_runtime_config_key_no_params(self, test_component):
#         """Test building runtime config key without parameters."""
#         key = test_component._TestConfigurableComponent__build_runtime_config_key("test_config", {})
#         expected = f"{test_component.component_name()}.test_config"
#         assert key == expected

#     def test_build_runtime_config_key_with_params(self, test_component):
#         """Test building runtime config key with parameters."""
#         key = test_component._TestConfigurableComponent__build_runtime_config_key(
#             "optional_config",
#             {"param1": "value1", "param2": 42}
#         )
#         expected = f"{test_component.component_name()}.optional_config.param1:value1,param2:42"
#         assert key == expected

#     def test_build_runtime_config_key_with_delimiters(self, test_component_with_delimiters):
#         """Test building runtime config key with delimiter characters."""
#         component = test_component_with_delimiters
#         key = component._TestConfigurableComponent__build_runtime_config_key(
#             "config.with.dots",
#             {"param:with:colons": "value:with:colons", "param,with,commas": 42}
#         )
#         # Should escape the delimiters in both config name and parameters
#         expected = f"{component.component_name()}.config__dot_literal__with__dot_literal__dots.param__colon_literal__with__colon_literal__colons:value__colon_literal__with__colon_literal__colons,param__comma_literal__with__comma_literal__commas:42"
#         assert key == expected

#     def test_deserialize_runtime_config_key_no_params(self, test_component):
#         """Test deserializing runtime config key without parameters."""
#         key = f"{test_component.component_name()}.test_config"
#         config_key, params = test_component._TestConfigurableComponent__deserialize_runtime_config_key(key)
#         assert config_key == "test_config"
#         assert params == {}

#     def test_deserialize_runtime_config_key_with_params(self, test_component):
#         """Test deserializing runtime config key with parameters."""
#         key = f"{test_component.component_name()}.optional_config.param1:value1,param2:42"
#         config_key, params = test_component._TestConfigurableComponent__deserialize_runtime_config_key(key)
#         assert config_key == "optional_config"
#         assert params == {"param1": "value1", "param2": "42"}

#     def test_deserialize_runtime_config_key_with_delimiters(self, test_component_with_delimiters):
#         """Test deserializing runtime config key with delimiter characters."""
#         component = test_component_with_delimiters
#         key = f"{component.component_name()}.config__dot_literal__with__dot_literal__dots.param__colon_literal__with__colon_literal__colons:value__colon_literal__with__colon_literal__colons,param__comma_literal__with__comma_literal__commas:42"
#         config_key, params = component._TestConfigurableComponent__deserialize_runtime_config_key(key)
#         assert config_key == "config.with.dots"
#         assert params == {"param:with:colons": "value:with:colons", "param,with,commas": "42"}

#     def test_deserialize_runtime_config_key_invalid_format(self, test_component):
#         """Test deserializing runtime config key with invalid format."""
#         key = "invalid.key.format"
#         with pytest.raises(IndexError):
#             test_component._TestConfigurableComponent__deserialize_runtime_config_key(key)


# class TestConfigurableComponentConfigOperations:
#     """Test config get/set/delete operations."""

#     @patch('snuba.configs.configuration.get_runtime_config')
#     def test_get_config_value_default(self, mock_get_runtime_config, test_component):
#         """Test getting config value with default."""
#         mock_get_runtime_config.return_value = None

#         value = test_component.get_config_value("test_config")
#         assert value == 100  # Default value

#         mock_get_runtime_config.assert_called_once()

#     @patch('snuba.configs.configuration.get_runtime_config')
#     def test_get_config_value_with_params(self, mock_get_runtime_config, test_component):
#         """Test getting config value with parameters."""
#         mock_get_runtime_config.return_value = "custom_value"

#         value = test_component.get_config_value(
#             "optional_config",
#             params={"param1": "value1", "param2": 42}
#         )
#         assert value == "custom_value"

#         mock_get_runtime_config.assert_called_once()

#     @patch('snuba.configs.configuration.set_runtime_config')
#     def test_set_config_value(self, mock_set_runtime_config, test_component):
#         """Test setting config value."""
#         test_component.set_config_value("test_config", 200)

#         mock_set_runtime_config.assert_called_once()
#         call_args = mock_set_runtime_config.call_args
#         assert call_args[1]['force'] is True

#     @patch('snuba.configs.configuration.set_runtime_config')
#     def test_set_config_value_with_params(self, mock_set_runtime_config, test_component):
#         """Test setting config value with parameters."""
#         test_component.set_config_value(
#             "optional_config",
#             "new_value",
#             params={"param1": "value1", "param2": 42}
#         )

#         mock_set_runtime_config.assert_called_once()

#     @patch('snuba.configs.configuration.delete_runtime_config')
#     def test_delete_config_value(self, mock_delete_runtime_config, test_component):
#         """Test deleting config value."""
#         test_component.delete_config_value("test_config")

#         mock_delete_runtime_config.assert_called_once()

#     @patch('snuba.configs.configuration.delete_runtime_config')
#     def test_delete_config_value_with_params(self, mock_delete_runtime_config, test_component):
#         """Test deleting config value with parameters."""
#         test_component.delete_config_value(
#             "optional_config",
#             params={"param1": "value1", "param2": 42}
#         )

#         mock_delete_runtime_config.assert_called_once()

#     def test_get_config_value_invalid_config(self, test_component):
#         """Test getting config value with invalid config key."""
#         with pytest.raises(InvalidConfig):
#             test_component.get_config_value("invalid_config")

#     def test_set_config_value_invalid_config(self, test_component):
#         """Test setting config value with invalid config key."""
#         with pytest.raises(InvalidConfig):
#             test_component.set_config_value("invalid_config", "value")

#     def test_delete_config_value_invalid_config(self, test_component):
#         """Test deleting config value with invalid config key."""
#         with pytest.raises(InvalidConfig):
#             test_component.delete_config_value("invalid_config")


# class TestConfigurableComponentConfigOverrides:
#     """Test default config overrides functionality."""

#     def test_get_overridden_additional_config_defaults(self, test_component):
#         """Test that default config overrides are applied correctly."""
#         component = test_component

#         # Check that the overridden configs have the new default values
#         definitions = component.additional_config_definitions()
#         additional_config = next(d for d in definitions if d.name == "additional_config")
#         assert additional_config.default is False

#     def test_get_overridden_additional_config_defaults_partial_override(self, test_component):
#         """Test that partial overrides work correctly."""
#         overrides = {"additional_config": False}
#         overridden_definitions = test_component._get_overridden_additional_config_defaults(overrides)

#         # Find the overridden config
#         additional_config = next(d for d in overridden_definitions if d.name == "additional_config")
#         assert additional_config.default is False

#         # Other configs should remain unchanged
#         test_config = next(d for d in overridden_definitions if d.name == "test_config", None)
#         assert test_config is None  # test_config is not in additional_config_definitions


# class TestConfigurableComponentConfigRetrieval:
#     """Test config retrieval methods."""

#     @patch('snuba.configs.configuration.get_all_runtime_configs')
#     def test_get_current_configs(self, mock_get_all_runtime_configs, test_component):
#         """Test getting current configs."""
#         # Mock runtime configs
#         mock_get_all_runtime_configs.return_value = {
#             f"{test_component.component_name()}.test_config": 150,
#             f"{test_component.component_name()}.optional_config.param1:value1,param2:42": "custom_value",
#         }

#         configs = test_component.get_current_configs()

#         # Should return all configs (including those with default values)
#         assert len(configs) == 3  # test_config, optional_config, additional_config

#         # Find the configs with runtime values
#         test_config = next(c for c in configs if c["name"] == "test_config")
#         optional_config = next(c for c in configs if c["name"] == "optional_config")

#         assert test_config["value"] == 150
#         assert optional_config["value"] == "custom_value"
#         assert optional_config["params"] == {"param1": "value1", "param2": "42"}

#     def test_get_optional_config_definitions_json(self, test_component):
#         """Test getting optional config definitions as JSON."""
#         optional_configs = test_component.get_optional_config_definitions_json()

#         # Should only include configs with parameters
#         assert len(optional_configs) == 1
#         assert optional_configs[0]["name"] == "optional_config"
#         assert "params" in optional_configs[0]
#         assert len(optional_configs[0]["params"]) == 2


# class TestConfigurableComponentErrorHandling:
#     """Test error handling in ConfigurableComponent."""

#     @patch('snuba.configs.configuration.get_all_runtime_configs')
#     def test_get_current_configs_deserialization_error(self, mock_get_all_runtime_configs, test_component):
#         """Test handling of deserialization errors in get_current_configs."""
#         # Mock runtime configs with invalid format
#         mock_get_all_runtime_configs.return_value = {
#             f"{test_component.component_name()}.invalid_format": "value",
#         }

#         # Should not raise an exception, but log the error
#         configs = test_component.get_current_configs()

#         # Should return configs with default values
#         assert len(configs) == 3  # All default configs

#     def test_get_config_value_validation_disabled(self, test_component):
#         """Test getting config value with validation disabled."""
#         # Should not raise InvalidConfig even with invalid config key
#         with patch('snuba.configs.configuration.get_runtime_config') as mock_get:
#             mock_get.return_value = "some_value"
#             value = test_component.get_config_value("invalid_config", validate=False)
#             assert value == "some_value"

#     def test_set_config_value_with_user(self, test_component):
#         """Test setting config value with user parameter."""
#         with patch('snuba.configs.configuration.set_runtime_config') as mock_set:
#             test_component.set_config_value("test_config", 200, user="test_user")

#             call_args = mock_set.call_args
#             assert call_args[1]['user'] == "test_user"

#     def test_delete_config_value_with_user(self, test_component):
#         """Test deleting config value with user parameter."""
#         with patch('snuba.configs.configuration.delete_runtime_config') as mock_delete:
#             test_component.delete_config_value("test_config", user="test_user")

#             call_args = mock_delete.call_args
#             assert call_args[1]['user'] == "test_user"


# class TestConfigurableComponentIntegration:
#     """Integration tests for ConfigurableComponent."""

#     def test_full_config_lifecycle(self, test_component):
#         """Test the full lifecycle of a configuration."""
#         # 1. Get initial value (should be default)
#         initial_value = test_component.get_config_value("test_config")
#         assert initial_value == 100

#         # 2. Set a new value
#         test_component.set_config_value("test_config", 200)

#         # 3. Get the new value
#         new_value = test_component.get_config_value("test_config")
#         assert new_value == 200

#         # 4. Delete the value (should reset to default)
#         test_component.delete_config_value("test_config")

#         # 5. Get value after deletion (should be default again)
#         final_value = test_component.get_config_value("test_config")
#         assert final_value == 100

#     def test_optional_config_lifecycle(self, test_component):
#         """Test the full lifecycle of an optional configuration."""
#         # 1. Set an optional config with parameters
#         test_component.set_config_value(
#             "optional_config",
#             "custom_value",
#             params={"param1": "value1", "param2": 42}
#         )

#         # 2. Get the optional config
#         value = test_component.get_config_value(
#             "optional_config",
#             params={"param1": "value1", "param2": 42}
#         )
#         assert value == "custom_value"

#         # 3. Delete the optional config
#         test_component.delete_config_value(
#             "optional_config",
#             params={"param1": "value1", "param2": 42}
#         )

#         # 4. Get value after deletion (should be default)
#         final_value = test_component.get_config_value(
#             "optional_config",
#             params={"param1": "value1", "param2": 42}
#         )
#         assert final_value == "default_value"

#     def test_component_with_delimiters_lifecycle(self, test_component_with_delimiters):
#         """Test the full lifecycle with delimiter characters."""
#         component = test_component_with_delimiters

#         # 1. Set a config with delimiter characters
#         component.set_config_value(
#             "config.with.dots",
#             "custom_value",
#             params={"param:with:colons": "value:with:colons", "param,with,commas": 42}
#         )

#         # 2. Get the config
#         value = component.get_config_value(
#             "config.with.dots",
#             params={"param:with:colons": "value:with:colons", "param,with,commas": 42}
#         )
#         assert value == "custom_value"

#         # 3. Delete the config
#         component.delete_config_value(
#             "config.with.dots",
#             params={"param:with:colons": "value:with:colons", "param,with,commas": 42}
#         )

#         # 4. Get value after deletion (should be default)
#         final_value = component.get_config_value(
#             "config.with.dots",
#             params={"param:with:colons": "value:with:colons", "param,with,commas": 42}
#         )
#         assert final_value == "default"
