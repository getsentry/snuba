from typing import Type, cast

import pytest

from snuba.utils.registered_class import InvalidConfigKeyError, RegisteredClass


def test_register() -> None:
    class Foo(metaclass=RegisteredClass):
        config_key = "base"

    class Bar(Foo):
        config_key = "Bar"

    assert Bar.config_key == "Bar"
    assert Foo.from_name("Bar") is Bar
    with pytest.raises(InvalidConfigKeyError):
        Foo.from_name("Foo")


def test_register_different() -> None:
    class X(metaclass=RegisteredClass):
        config_key = "X"

    class Y(X):
        config_key = "Y"

    with pytest.raises(InvalidConfigKeyError):
        X.from_name("X")
    with pytest.raises(InvalidConfigKeyError):
        X.from_name("Bar")
    assert X.from_name("Y") is Y
    assert Y.from_name("Y") is Y


def test_custom_key() -> None:
    class CustomKey(metaclass=RegisteredClass):
        config_key = "custom_af"

    class ExtraCustom(CustomKey):
        config_key = "cool_key"

    assert CustomKey.from_name("cool_key") is ExtraCustom
    with pytest.raises(InvalidConfigKeyError):
        CustomKey.from_name("custom_af")


class TypedFromName(metaclass=RegisteredClass):
    config_key = "base"

    @classmethod
    def get_from_name(cls, name: str) -> Type["TypedFromName"]:
        # NOTE: This method cannot be type safe without doing this cast. Such is the nature of metaprogramming
        res = cls.from_name(name)
        return cast(Type[TypedFromName], res)


class ExtraName(TypedFromName):
    config_key = "extra_name"


def get_from_name(name: str) -> Type[TypedFromName]:
    return TypedFromName.get_from_name(name)


def test_override_from_name() -> None:
    assert get_from_name("extra_name") is ExtraName
    with pytest.raises(InvalidConfigKeyError):
        assert get_from_name("base") is None
