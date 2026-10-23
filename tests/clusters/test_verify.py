import pytest

from snuba.clusters.cluster import ClickhouseCluster


@pytest.mark.parametrize(
    "raw,expected",
    [
        (None, None),
        (True, True),
        (False, False),
        ("true", True),
        ("1", True),
        ("false", False),
        ("FALSE", False),
        ("0", False),
        (" false ", False),
        ("", True),
        ("yes", True),
        ("garbage", True),
    ],
)
def test_get_verify_coercion(raw: bool | str | None, expected: bool | None) -> None:
    cluster = ClickhouseCluster(
        "127.0.0.1",
        8001,
        "default",
        "",
        "default",
        True,
        None,
        raw,
        {"events"},
        True,
    )

    assert cluster.get_verify() == expected
