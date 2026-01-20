"""These tests test the auto-importing functionality in snuba for RegisteredClass(es)

In order to do this, there are two subdirectories which are only used for testing

snuba/
├─ tests/
│  ├─ utils/
│  │  ├─ test_import_submodules.py <-- (this file)
│  │  ├─ test_package_no_import/ <-- RegisteredClass with no auto importing
│  │  │  ├─ __init__.py
│  │  │  ├─ a.py
│  │  │  ├─ b.py
│  │  │  ├─ c.py
│  │  │  ├─ garbage
│  │  ├─ test_package_auto_import/ <-- RegisteredClass no auto importing
│  │  │  ├─ __init__.py <-- import_submodules_in_directory called here
│  │  │  ├─ a.py
│  │  │  ├─ b.py
│  │  │  ├─ c.py
│  │  │  ├─ garbage

the files in the subdirectories are absolutely the same except that test_package_auto_import calls
`import_submodules_in_directory` in the __init__.py file.
"""

import os

import pytest

from snuba.utils.registered_class import (
    InvalidConfigKeyError,
    import_submodules_in_directory,
)

dir_path = os.path.dirname(os.path.realpath(__file__))


def test_no_import_no_lookup() -> None:
    from tests.utils.test_package_no_import import SomeBase

    # there was not auto importoing, thus none of the RegisteredClass(es)
    # can be looked up by name
    for prefix in ["A", "B", "C"]:
        with pytest.raises(InvalidConfigKeyError):
            assert SomeBase.class_from_name(prefix).__name__ == prefix


def test_import_submodules_manually() -> None:
    from tests.utils.test_package_no_import import SomeBase

    # we import the submodules explicitly and name lookup works
    import_submodules_in_directory(
        os.path.join(dir_path, "test_package_no_import"),
        "tests.utils.test_package_no_import",
    )
    for prefix in ["A", "B", "C"]:
        assert SomeBase.class_from_name(prefix).__name__ == prefix


def test_import_submodules_automatically() -> None:
    # this file will automatically import its submodules, thus all name
    # lookups will work
    from tests.utils.test_package_auto_import import SomeBase

    for prefix in ["A", "B", "C"]:
        assert SomeBase.class_from_name(prefix).__name__ == prefix
