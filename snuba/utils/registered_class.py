from abc import ABCMeta
from typing import Any, Dict, Generic, Optional, Tuple, Type, TypeVar, cast

T = TypeVar("T")


class _ClassRegistry(Generic[T]):
    """Keep a mapping of classes to their names"""

    def __init__(self) -> None:
        self.__mapping: Dict[str, Type[T]] = {}

    def register_class(self, cls: Type[T]) -> None:
        key_func = getattr(cls, "registry_key", None)
        if key_func:
            key = key_func()
        else:
            key = cls.__name__
        existing_class = self.__mapping.get(key)
        if not existing_class:
            self.__mapping[key] = cls

    def get_class_from_name(self, cls_name: str) -> Optional[Type[T]]:
        return self.__mapping.get(cls_name)


class RegisteredClass(ABCMeta):
    """Metaclass for making classes that can be looked up by name"""

    def __new__(  # type: ignore
        cls, name: str, bases: Tuple[Type[Any]], dct: Dict[str, Any]
    ):
        res = super().__new__(cls, name, bases, dct)
        if not hasattr(res, "_registry"):
            setattr(res, "_registry", _ClassRegistry())
        else:
            getattr(res, "_registry").register_class(res)
        return res

    def from_name(self, name: str) -> Optional[Type[Any]]:
        return cast(
            Optional[Type[Any]], getattr(self, "_registry").get_class_from_name(name)
        )

    def registry_key(self) -> str:
        return self.__name__
