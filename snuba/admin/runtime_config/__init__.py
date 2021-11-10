from typing import Union


def get_config_type_from_value(value: Union[str, int, float]) -> str:
    if isinstance(value, str):
        return "string"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    raise ValueError("Unexpected config type")
