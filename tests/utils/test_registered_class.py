from snuba.utils.registered_class import RegisteredClass


def test_register():
    class Foo(metaclass=RegisteredClass):
        def bar(self):
            return 1

    class Bar(Foo):
        pass

    assert Foo.from_name("Bar") is Bar
    assert Foo.from_name("Foo") is None


def test_register_different():
    class X(metaclass=RegisteredClass):
        pass

    class Y(X):
        pass

    assert X.from_name("X") is None
    assert X.from_name("Bar") is None
    assert X.from_name("Y") is Y
    assert Y.from_name("Y") is Y


def test_custom_key():
    class CustomKey(metaclass=RegisteredClass):
        @classmethod
        def registry_key(cls):
            return "custom_af"

    class ExtraCustom(CustomKey):
        @classmethod
        def registry_key(cls):
            return "cool_key"

    assert CustomKey.from_name("cool_key") is ExtraCustom
    assert CustomKey.from_name("custom_af") is None
