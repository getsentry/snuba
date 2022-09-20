from snuba.utils.registered_class import RegisteredClass


class SomeBase(metaclass=RegisteredClass):
    @classmethod
    def config_key(cls):
        return cls.__name__
