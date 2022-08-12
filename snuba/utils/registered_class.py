from abc import ABCMeta
from typing import Any, Dict, Optional, Tuple, Type, cast


class NoConfigKeyError(Exception):
    pass


class _ClassRegistry:
    """Keep a mapping of classes to their names"""

    def __init__(self) -> None:
        self.__mapping: Dict[str, RegisteredClass] = {}

    def register_class(self, cls: "RegisteredClass") -> None:
        key = cls.config_key
        existing_class = self.__mapping.get(key)
        if not existing_class:
            self.__mapping[key] = cls

    def get_class_from_name(self, cls_name: str) -> Optional["RegisteredClass"]:
        return self.__mapping.get(cls_name)


class RegisteredClass(ABCMeta):
    """Metaclass for making classes that can be looked up by name
    Usage:
        class SomeGenericClass(metaclass=RegisteredClass):
            pass

        class Subclass(SomeGenericClass):
            def config_key(self) -> str:
                return "sub_class"


        assert SomeGenericClass.from_name("sub_class") is Subclass

    Notes:
        The base class cannot be looked up by name, only subclasses
    """

    config_key: str

    def __new__(cls, name: str, bases: Tuple[Type[Any]], dct: Dict[str, Any]) -> Any:
        res = super().__new__(cls, name, bases, dct)
        if not hasattr(res, "config_key"):
            raise NoConfigKeyError(
                "RegisteredClass(es) must define the `config-key` property"
            )
        if not hasattr(res, "_registry"):
            setattr(res, "_registry", _ClassRegistry())
        else:
            getattr(res, "_registry").register_class(res)
        return res

    def from_name(self, name: str) -> Optional[Type[Any]]:
        return cast(
            Optional[Type[Any]],
            getattr(self, "_registry").get_class_from_name(name),
        )
