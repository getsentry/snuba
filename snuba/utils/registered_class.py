from typing import Dict, Generic, Optional, Type, TypeVar

T = TypeVar("T")


class _ClassRegistry(Generic[T]):
    """Keep a mapping of classes to their names"""

    def __init__(self) -> None:
        self.__mapping: Dict[str, Type[T]] = {}

    def register_class(self, cls: Type[T]) -> None:
        existing_class = self.__mapping.get(cls.__name__)
        if not existing_class:
            self.__mapping[cls.__name__] = cls

    def get_class_by_name(self, cls_name: str) -> Optional[Type[T]]:
        return self.__mapping.get(cls_name)


class RegisteredClass(type):
    """Metaclass for making classes that can be looked up by name"""

    _registry: _ClassRegistry = _ClassRegistry()

    def __new__(cls, name, bases, dct):
        res = super().__new__(cls, name, bases, dct)
        if not bases:
            res._registry = _ClassRegistry()
        else:
            res._registry.register_class(res)

        return res

    def from_name(self, name: str) -> Optional[Type]:
        return self._registry.get_class_by_name(name)
