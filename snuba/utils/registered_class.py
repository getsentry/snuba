from __future__ import annotations

import importlib
import os
from abc import ABCMeta
from typing import Any, Dict, Tuple, Type, cast


class NoConfigKeyError(Exception):
    pass


class InvalidConfigKeyError(Exception):
    pass


class RegisteredClassNameCollisionError(Exception):
    pass


class _ClassRegistry:
    """Keep a mapping of classes to their names"""

    def __init__(self) -> None:
        self.__mapping: Dict[str, RegisteredClass] = {}

    def register_class(self, cls: "RegisteredClass") -> None:
        key = cls.config_key()
        existing_class = self.__mapping.get(key)
        if not existing_class:
            self.__mapping[key] = cls
        else:
            raise RegisteredClassNameCollisionError(
                f"Class with name {key} already exists in the registry, change the config_key property in the class {cls} or {existing_class}"
            )

    def get_class_from_name(self, config_key: str) -> "RegisteredClass":
        res = self.__mapping.get(config_key)
        if res is None:
            raise InvalidConfigKeyError(
                f"A class with config_key={config_key} does not exist in the registry. Was it ever imported? Are you trying to look up the base class (not supported)?"
            )
        return res


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

    def config_key(cls) -> str:
        raise NotImplementedError

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

    def class_from_name(self, name: str) -> Type[Any]:
        return cast(
            Type[Any],
            getattr(self, "_registry").get_class_from_name(name),
        )


TModule = object


def import_submodules_in_directory(
    directory_path: str, package_root: str
) -> list[TModule]:
    """Given a directory path, import all the modules in that directory.
    This is meant to be used in concert with RegisteredClass(es) as a RegisteredClass
    needs to be imported to be registered.

    directory_path: a path to the directory where all the RegisteredClass(es) are defined
    package_root: the module path minus the final module name


    Example:
    --------
        imagine the following directory structure

        snuba/
        ├─ query_processors/
        │  ├─ __init__.py
        │  ├─ some_other_processor.py
        │  ├─ some_processor.py

        __init__.py contains a RegisteredClass:
        (it can be outside the __init__.py, there's no technical reason it has to but convention makes things convenient)

        >>> class QueryProcessor(metaclass=RegisteredClass):
        >>>     pass

        The other files in the `query_processors` directory define subclasses of the QueryProcessor class.

        In the `__init__.py` simply add:

        >>> import_submodules_in_directory(
        >>>     os.path.dirname(os.path.realpath(__file__)),
        >>>     "snuba.query_processors.snuba"
        >>> )

        As the __init__.py finishes importing, all the subclasses will be registered in the subdirectory, thus

        >>> from snuba.query_processors import QueryProcessor
        >>> QueryProcessor.class_from_name("some_other_processor")

        will return the correct class just by virtue of having imported `QueryProcessor`
    """
    imported_modules: list[object] = []
    for fname in os.listdir(directory_path):
        if fname == "__init__.py":
            continue
        module_name = fname.replace(".py", "")
        module_str = f"{package_root}.{module_name}"
        imported_modules.append(importlib.import_module(module_str))
    return imported_modules
