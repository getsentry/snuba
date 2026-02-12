from typing import Optional, TypedDict, Union

ConfigType = Union[str, int, float]

ConfigChange = TypedDict(
    "ConfigChange",
    {
        "key": str,
        "timestamp": float,
        "user": Optional[str],
        "before": Optional[str],
        "beforeType": Optional[str],
        "after": Optional[str],
        "afterType": Optional[str],
    },
)


def get_config_type_from_value(value: Optional[Union[str, int, float]]) -> Optional[str]:
    if value is None:
        return None

    if isinstance(value, str):
        return "string"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    raise ValueError("Unexpected config type")
