from typing import Any, Dict, List, Optional, Type, TypedDict, Union, cast

from snuba.utils.snuba_exception import SnubaException

# mypy has not figured out recursive types yet so this can't be totally typesafe
JsonSerializable = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]


class SnubaExceptionDict(TypedDict):
    __type__: str
    __name__: str
    __message__: str
    __extra_data__: Dict[str, JsonSerializable]


class _ExceptionRegistry:
    """Keep a mapping of SnubaExceptions to their names"""

    def __init__(self) -> None:
        self.__mapping: Dict[str, Type["SnubaException"]] = {}

    def register_class(self, cls: Type["SnubaException"]) -> None:
        existing_class = self.__mapping.get(cls.__name__)
        if not existing_class:
            self.__mapping[cls.__name__] = cls

    def get_class_by_name(self, cls_name: str) -> Optional[Type["SnubaException"]]:
        return self.__mapping.get(cls_name)


_REGISTRY = None


def _get_registry() -> _ExceptionRegistry:
    global _REGISTRY
    # NOTE (Should this be protected by a mutex? I'm thinking it should be fine)
    # given that this will be instatiated with the python AST loading
    if _REGISTRY is None:
        _REGISTRY = _ExceptionRegistry()
    return _REGISTRY


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
        assert edict["__type__"] == "SnubaException"
        defined_exception = _get_registry().get_class_by_name(edict.get("__name__", ""))
        if defined_exception is not None:
            return defined_exception(
                message=edict.get("__message__", ""), **edict.get("__extra_data__", {})
            )
        return cast(
            SnubaException,
            type(edict["__name__"], (cls,), {})(
                message=edict.get("__message__", ""), **edict.get("__extra_data__", {})
            ),
        )

    def __init_subclass__(cls) -> None:
        _get_registry().register_class(cls)
        return super().__init_subclass__()

    @classmethod
    def from_standard_exception_instance(cls, exc: Exception) -> "SnubaException":
        if isinstance(exc, cls):
            return exc
        return cls.from_dict(
            {
                "__type__": "SnubaException",
                "__name__": exc.__class__.__name__,
                "__message__": str(exc),
                "__extra_data__": {"from_standard_exception": True},
            }
        )
