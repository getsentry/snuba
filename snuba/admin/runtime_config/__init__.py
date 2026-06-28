from typing import TypedDict

ConfigType = str | int | float


class ConfigChange(TypedDict):
    key: str
    timestamp: float
    user: str | None
    before: str | None
    beforeType: str | None
    after: str | None
    afterType: str | None


def get_config_type_from_value(value: str | int | float | None) -> str | None:
    if value is None:
        return None

    if isinstance(value, str):
        return "string"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    raise ValueError("Unexpected config type")
