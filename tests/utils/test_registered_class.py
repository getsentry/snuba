from typing import Type, cast

import pytest

from snuba.utils.registered_class import (
    InvalidConfigKeyError,
    RegisteredClass,
    RegisteredClassNameCollisionError,
)


def test_register() -> None:
    class Foo(metaclass=RegisteredClass):
        @classmethod
        def config_key(cls) -> str:
            return "base"

    class Bar(Foo):
        @classmethod
        def config_key(cls) -> str:
            return "Bar"

    assert Bar.config_key() == "Bar"
    assert Foo.class_from_name("Bar") is Bar
    with pytest.raises(InvalidConfigKeyError):
        Foo.class_from_name("Foo")


def test_name_collision() -> None:
    def register_identical_keys() -> None:
        class A(metaclass=RegisteredClass):
            @classmethod
            def config_key(cls) -> str:
                return "base"

        class B(A):
            @classmethod
            def config_key(cls) -> str:
                return "key"

        class C(A):
            @classmethod
            def config_key(cls) -> str:
                return "key"

    with pytest.raises(RegisteredClassNameCollisionError):
        register_identical_keys()


def test_register_different() -> None:
    class X(metaclass=RegisteredClass):
        @classmethod
        def config_key(cls) -> str:
            return "X"

    class Y(X):
        @classmethod
        def config_key(cls) -> str:
            return "Y"

    with pytest.raises(InvalidConfigKeyError):
        X.class_from_name("X")
    with pytest.raises(InvalidConfigKeyError):
        X.class_from_name("Bar")
    assert X.class_from_name("Y") is Y
    assert Y.class_from_name("Y") is Y


def test_custom_key() -> None:
    class CustomKey(metaclass=RegisteredClass):
        @classmethod
        def config_key(cls) -> str:
            return "custom_af"

    class ExtraCustom(CustomKey):
        @classmethod
        def config_key(cls) -> str:
            return "cool_key"

    class SubclassCustom(ExtraCustom):
        @classmethod
        def config_key(cls) -> str:
            return "subclass_key"

    assert CustomKey.class_from_name("cool_key") is ExtraCustom
    with pytest.raises(InvalidConfigKeyError):
        CustomKey.class_from_name("custom_af")

    assert CustomKey.class_from_name("subclass_key") is SubclassCustom


def test_class_registry_all_names() -> None:
    class TestBase(metaclass=RegisteredClass):
        @classmethod
        def config_key(cls) -> str:
            return cls.__name__

    class TestClass1(TestBase):
        pass

    class TestClass2(TestBase):
        pass

    expected_names = ["TestClass1", "TestClass2"]
    assert set(TestBase.all_names()) == set(expected_names)
    assert isinstance(TestBase.all_names(), list)


class TypedFromName(metaclass=RegisteredClass):
    @classmethod
    def config_key(cls) -> str:
        return "base"

    @classmethod
    def get_class_from_name(cls, name: str) -> Type["TypedFromName"]:
        # NOTE: This method cannot be type safe without doing this cast. Such is the nature of metaprogramming
        res = cls.class_from_name(name)
        return cast(Type[TypedFromName], res)


class ExtraName(TypedFromName):
    @classmethod
    def config_key(cls) -> str:
        return "extra_name"


def get_from_name(name: str) -> Type[TypedFromName]:
    return TypedFromName.get_class_from_name(name)


def test_override_from_name() -> None:
    assert get_from_name("extra_name") is ExtraName
    with pytest.raises(InvalidConfigKeyError):
        assert get_from_name("base") is None
