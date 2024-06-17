import pytest

from snuba.migrations.autogeneration import generate


def test_basic() -> None:
    old_storage, new_storage = generate(
        "snuba/datasets/configuration/events/storages/errors.yaml"
    )
    assert old_storage, new_storage


def test_error() -> None:
    with pytest.raises(ValueError):
        generate("~/hello.txt")
