from typing import Optional, Type, cast

from snuba.utils.registered_class import RegisteredClass


def test_register() -> None:
    class Foo(metaclass=RegisteredClass):
        config_key = "base"

    class Bar(Foo):
        config_key = "Bar"

    assert Bar.config_key == "Bar"
    assert Foo.from_name("Bar") is Bar
    assert Foo.from_name("Foo") is None


def test_register_different() -> None:
    class X(metaclass=RegisteredClass):
        config_key = "X"

    class Y(X):
        config_key = "Y"

    assert X.from_name("X") is None
    assert X.from_name("Bar") is None
    assert X.from_name("Y") is Y
    assert Y.from_name("Y") is Y


def test_custom_key() -> None:
    class CustomKey(metaclass=RegisteredClass):
        config_key = "custom_af"

    class ExtraCustom(CustomKey):
        config_key = "cool_key"

    assert CustomKey.from_name("cool_key") is ExtraCustom
    assert CustomKey.from_name("custom_af") is None


class TypedFromName(metaclass=RegisteredClass):
    config_key = "base"

    @classmethod
    def get_from_name(cls, name: str) -> Optional[Type["TypedFromName"]]:
        # NOTE: This method cannot be type safe without doing this cast. Such is the nature of metaprogramming
        res = cls.from_name(name)
        return cast(Type[TypedFromName], res)


class ExtraName(TypedFromName):
    config_key = "extra_name"


def get_from_name(name: str) -> Optional[Type[TypedFromName]]:
    return TypedFromName.get_from_name(name)


def test_override_from_name() -> None:
    assert get_from_name("extra_name") is ExtraName
    assert get_from_name("base") is None
