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
