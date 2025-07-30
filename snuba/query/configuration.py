from dataclasses import dataclass, field
from typing import Any

from snuba.state import delete_config as delete_runtime_config


class InvalidConfig(Exception):
    pass


@dataclass()
class Configuration:
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
            "value": value if value is not None else self.default,
            "params": params,
        }


class ConfigurableComponent:

    # This component builds redis strings that are delimited by dots, commas, colons
    # in order to allow those characters to exist in config we replace them with their
    # counterparts on write/read. It may be better to just replace our serialization with JSON
    # instead of what we're doing but this is where we're at rn 1/10/24
    __KEY_DELIMITERS_TO_ESCAPE_SEQUENCES = {
        ".": "__dot_literal__",
        ",": "__comma_literal__",
        ":": "__colon_literal__",
    }

    def component_name(self) -> str:
        # what is this configurable component?
        # allocation policy? routing strategy? strategy selector?
        return self.__class__.__name__

    def component_namespace(self) -> str:
        # a way to uniquely identify the component
        # so that a configuration namespace can
        # be created for it
        raise NotImplementedError

    def get_default_config_definitions(self) -> list[Configuration]:
        """Returns a list of default config definitions for this AllocationPolicy."""
        raise NotImplementedError

    def get_additional_config_definitions(self) -> list[Configuration]:
        """Returns a list of additional config definitions for this AllocationPolicy."""
        raise NotImplementedError

    @property
    def runtime_config_prefix(self) -> str:
        raise NotImplementedError

    def get_configurations(self) -> dict[str, Configuration]:
        """Returns a dictionary of config definitions on this AllocationPolicy."""
        return {
            config.name: config
            for config in self.get_default_config_definitions()
            + self.get_additional_config_definitions()
        }

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

    def _validate_config_params(
        self, config_key: str, params: dict[str, Any], value: Any = None
    ) -> Configuration:
        definitions = self.get_configurations()

        class_name = self.__class__.__name__

        # config doesn't exist
        if config_key not in definitions:
            raise InvalidConfig(
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

    def __escape_delimiter_chars(self, key: str) -> str:
        if not isinstance(key, str):
            return key
        for (
            delimiter_char,
            escape_sequence,
        ) in self.__KEY_DELIMITERS_TO_ESCAPE_SEQUENCES.items():
            if escape_sequence in str(key):
                raise InvalidConfig(
                    f"{escape_sequence} is not a valid string for a config"
                )
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
        return f"{self.runtime_config_prefix}.{config}{parameters}"

    def delete_config_value(
        self,
        config_key: str,
        hash: str,
        params: dict[str, Any] = {},
        user: str | None = None,
    ) -> None:
        """
        Deletes an instance of an optional config on this AllocationPolicy.
        If this function is run on a required config, it resets the value to default instead.
        """
        self._validate_config_params(config_key, params)
        delete_runtime_config(
            key=self.__build_runtime_config_key(config_key, params),
            user=user,
            config_key=config_key,
        )
