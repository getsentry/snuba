"""
These are the end-to-end tests of the parsing pipeline as a whole.
The number of tests in this file should be kept to a minimum, just
a small number of representative cases for the entire pipeline.

If you have a new component to the pipeline to test, you should test it in isolation
by creating unit tests in ./unit_tests
"""


import pytest


def test_mql() -> None:
    pass


def test_formula_mql() -> None:
    pass


def test_snql() -> None:
    pass


@pytest.mark.skip()
def test_snql_composite() -> None:
    """
    We currently have no composite queries in our parse tests... aparently.
    """
    pass


def test_custom_processing() -> None:
    pass
