import pytest

from snuba.configs.configuration import (
    ConfigurableComponent,
    Configuration,
    InvalidConfig,
    ResourceIdentifier,
)
from snuba.datasets.storages.storage_key import StorageKey
from tests.configs.component_config import (
    delete_component_config,
    set_component_config,
)


class SomeConfigurableComponent(ConfigurableComponent):
    def __init__(self) -> None:
        self._default_config_definitions = [
            Configuration(
                name="default_config_1",
                description="A default configuration",
                value_type=int,
                default=100,
            ),
            Configuration(
                name="override_config_for_org_id",
                description="override config for org",
                value_type=int,
                default=-1,
                param_types={"organization_id": int},
            ),
        ]

    @classmethod
    def component_namespace(cls) -> str:
        return "SomeConfigurableComponent"

    def _get_default_config_definitions(self) -> list[Configuration]:
        return self._default_config_definitions

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
            Configuration(
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

    def test_invalid_default_value_type(self) -> None:
        """Test that Configuration with invalid default value type raises ValueError."""
        with pytest.raises(
            ValueError,
            match="Config item `invalid_config` expects type <class 'int'> got value `not_an_int` of type <class 'str'>",
        ):
            TestConfigurableComponentInvalidDefault()


class TestConfigurableComponentValidation:
    """Test configuration parameter validation."""

    def test_validate_config_params_valid_config(
        self, test_component: SomeConfigurableComponent
    ) -> None:
        """Test validation with valid config and parameters."""
        config = test_component._validate_config_params(
            config_key="override_config_for_org_id", params={"organization_id": 42}
        )
        assert config.name == "override_config_for_org_id"
        assert config.param_types == {"organization_id": int}

    def test_validate_config_params_invalid_config(
        self, test_component: SomeConfigurableComponent
    ) -> None:
        """Test validation with invalid config key."""
        with pytest.raises(
            InvalidConfig,
            match="'invalid_config' is not a valid config for SomeConfigurableComponent",
        ):
            test_component._validate_config_params("invalid_config", {})

    def test_validate_config_params_missing_required_params(
        self, test_component: SomeConfigurableComponent
    ) -> None:
        with pytest.raises(
            InvalidConfig,
            match="'override_config_for_org_id' missing required parameters: {'organization_id': 'int'} for SomeConfigurableComponent!",
        ):
            test_component._validate_config_params("override_config_for_org_id", {})

    def test_validate_config_params_wrong_param_type(
        self, test_component: SomeConfigurableComponent
    ) -> None:
        with pytest.raises(
            InvalidConfig, match="parameter 'organization_id' needs to be of type int"
        ):
            test_component._validate_config_params(
                "override_config_for_org_id", {"organization_id": "not_an_int"}
            )

    def test_validate_config_params_wrong_value_type(
        self, test_component: SomeConfigurableComponent
    ) -> None:
        with pytest.raises(InvalidConfig, match="value needs to be of type int"):
            test_component._validate_config_params(
                "override_config_for_org_id",
                {"organization_id": 42},
                value="not_an_int",
            )


@pytest.mark.redis_db
class TestConfigurableComponentConfigOperations:
    """Test config get/set/delete operations."""

    def test_get_config_value_default(self, test_component: SomeConfigurableComponent) -> None:
        """Test getting config value with default."""
        assert test_component.get_config_value("default_config_1") == 100  # Default value

    def test_get_config_value_with_params(self, test_component: SomeConfigurableComponent) -> None:
        """Test getting config value with parameters."""
        set_component_config(
            test_component, "override_config_for_org_id", 100, params={"organization_id": 10}
        )

        assert (
            test_component.get_config_value(
                "override_config_for_org_id", params={"organization_id": 10}
            )
            == 100
        )

    def test_set_config_value(self, test_component: SomeConfigurableComponent) -> None:
        """Test setting config value."""
        set_component_config(test_component, "default_config_1", 200)
        assert test_component.get_config_value("default_config_1") == 200

    def test_delete_config_value(self, test_component: SomeConfigurableComponent) -> None:
        """Test deleting config value."""
        config_key = "default_config_1"

        set_component_config(test_component, config_key=config_key, value=5)
        assert test_component.get_config_value(config_key=config_key) == 5

        delete_component_config(test_component, config_key=config_key)
        # back to default
        assert test_component.get_config_value(config_key=config_key) == 100

    def test_delete_config_value_with_params(
        self, test_component: SomeConfigurableComponent
    ) -> None:
        config_key = "override_config_for_org_id"
        params = {"organization_id": 10}

        set_component_config(test_component, config_key=config_key, value=100, params=params)
        assert test_component.get_config_value(config_key=config_key, params=params) == 100

        delete_component_config(test_component, config_key=config_key, params=params)
        # back to default
        assert test_component.get_config_value(config_key=config_key, params=params) == -1

    def test_get_config_value_invalid_config(
        self, test_component: SomeConfigurableComponent
    ) -> None:
        with pytest.raises(InvalidConfig):
            test_component.get_config_value("invalid_config")

    def test_set_config_value_invalid_config(
        self, test_component: SomeConfigurableComponent
    ) -> None:
        with pytest.raises(InvalidConfig):
            set_component_config(test_component, "invalid_config", "value")

    def test_delete_config_value_invalid_config(
        self, test_component: SomeConfigurableComponent
    ) -> None:
        with pytest.raises(InvalidConfig):
            delete_component_config(test_component, "invalid_config")
