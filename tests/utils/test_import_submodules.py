import os

import pytest

from snuba.utils.registered_class import (
    InvalidConfigKeyError,
    import_submodules_in_directory,
)

dir_path = os.path.dirname(os.path.realpath(__file__))


def test_no_import_no_lookup() -> None:
    from tests.utils.test_package_no_import import SomeBase  # type: ignore

    for prefix in ["A", "B", "C"]:
        with pytest.raises(InvalidConfigKeyError):
            assert SomeBase.class_from_name(prefix).__name__ == prefix


def test_import_submodules_manually() -> None:
    from tests.utils.test_package_no_import import SomeBase

    import_submodules_in_directory(
        os.path.join(dir_path, "test_package_no_import"),
        "tests.utils.test_package_no_import",
    )
    for prefix in ["A", "B", "C"]:
        assert SomeBase.class_from_name(prefix).__name__ == prefix


def test_import_submodules_automatically() -> None:
    from tests.utils.test_package_auto_import import SomeBase

    for prefix in ["A", "B", "C"]:
        assert SomeBase.class_from_name(prefix).__name__ == prefix
