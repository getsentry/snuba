from typing import Any, Dict, List, TypedDict, Union, cast

JsonSerializable = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]


class SnubaExceptionDict(TypedDict):
    __type__: str
    __name__: str
    __message__: str
    __extra_data__: Dict[str, JsonSerializable]


class SnubaException(Exception):
    def __init__(self, message: str, **extra_data: JsonSerializable) -> None:
        self.message = message
        self.extra_data = extra_data or {}

    def to_dict(self) -> SnubaExceptionDict:
        return {
            "__type__": "SnubaException",
            "__name__": self.__class__.__name__,
            "__message__": self.message,
            "__extra_data__": self.extra_data,
        }

    @classmethod
    def from_dict(cls, edict: SnubaExceptionDict) -> "SnubaException":
        return cast(
            SnubaException,
            type(edict["__name__"], (cls,), {})(
                message=edict.get("__message__", ""), **edict.get("__extra_data__", {})
            ),
        )
