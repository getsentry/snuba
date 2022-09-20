import os

from snuba.utils.registered_class import RegisteredClass, import_submodules_in_directory


class SomeBase(metaclass=RegisteredClass):
    @classmethod
    def config_key(cls):
        return cls.__name__


import_submodules_in_directory(
    os.path.dirname(os.path.realpath(__file__)), "tests.utils.test_package_auto_import"
)
