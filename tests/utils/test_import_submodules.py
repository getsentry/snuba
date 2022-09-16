import importlib
import os
import shutil
from typing import Any, Generator

import pytest

from snuba.utils.registered_class import (
    InvalidConfigKeyError,
    import_submodules_in_directory,
)

dir_path = os.path.dirname(os.path.realpath(__file__))
test_package_dir = os.path.join(dir_path, "test_package")


@pytest.fixture(scope="function")
def temp_package_directory() -> Generator[None, None, None]:
    os.mkdir(test_package_dir)
    init_file = """from snuba.utils.registered_class import RegisteredClass

class SomeBase(metaclass=RegisteredClass):
    @classmethod
    def config_key(cls):
        return cls.__name__
    """
    with open(os.path.join(test_package_dir, "__init__.py"), "w") as f:
        f.write(init_file)

    for prefix in ["A", "B", "C"]:
        with open(os.path.join(test_package_dir, f"{prefix.lower}.py"), "w") as f:
            f.write(
                f"""
from tests.utils.test_package import SomeBase

class {prefix}(SomeBase):
    pass
"""
            )

    importlib.invalidate_caches()
    yield
    shutil.rmtree(test_package_dir)
    importlib.invalidate_caches()


def test_no_import_no_lookup(temp_package_directory: Any) -> None:
    from tests.utils.test_package import SomeBase  # type: ignore

    for prefix in ["A", "B", "C"]:
        with pytest.raises(InvalidConfigKeyError):
            assert SomeBase.class_from_name(prefix).__name__ == prefix


def test_import_submodules(temp_package_directory: Any) -> None:
    from tests.utils.test_package import SomeBase

    import_submodules_in_directory(test_package_dir, "tests.utils.test_package")
    for prefix in ["A", "B", "C"]:
        assert SomeBase.class_from_name(prefix).__name__ == prefix
