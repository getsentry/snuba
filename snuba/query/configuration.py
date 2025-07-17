from dataclasses import field
from typing import Any


class Configuration:
    name: str
    description: str
    value_type: type
    default: Any
    param_types: dict[str, type] = field(default_factory=dict)


class ConfigurableComponent:
    def component_name(self) -> str:
        # what is this configurable component?
        # allocation policy? routing strategy? strategy selector?
        return self.__class__.__name__

    def component_namespace(self) -> str:
        # a way to uniquely identify the component
        # so that a configuration namespace can
        # be created for it
        raise NotImplementedError

    def get_configurations(self) -> list[Configuration]:
        raise NotImplementedError

    def get_config_value(
        self, config_key: str, params: dict[str, Any] = {}, validate: bool = True
    ) -> Any:
        pass

    def set_config_value(
        self,
        config_key: str,
        value: Any,
        params: dict[str, Any] = {},
        user: str | None = None,
    ) -> None:
        pass
