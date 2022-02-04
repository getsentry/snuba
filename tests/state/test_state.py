import random
import time
from collections import ChainMap
from functools import partial

import pytest

from snuba import state
from snuba.state import MismatchedTypeException, safe_dumps


class TestState:
    def setup_method(self):
        from snuba.web.views import application

        assert application.testing == True
        self.app = application.test_client()
        self.app.post = partial(self.app.post, headers={"referer": "test"})

    def test_config(self) -> None:
        state.set_config("foo", 1)
        state.set_configs({"bar": 2, "baz": 3})
        assert state.get_config("foo") == 1
        assert state.get_config("bar") == 2
        assert state.get_config("noexist", 4) == 4
        all_configs = state.get_all_configs()
        assert all(all_configs[k] == v for k, v in [("foo", 1), ("bar", 2), ("baz", 3)])
        assert state.get_configs(
            [("foo", 100), ("bar", 200), ("noexist", 300), ("noexist-2", None)]
        ) == [1, 2, 300, None]

        state.set_configs({"bar": "quux"}, force=True)
        all_configs = state.get_all_configs()
        assert all(
            all_configs[k] == v for k, v in [("foo", 1), ("bar", "quux"), ("baz", 3)]
        )

    def test_config_types(self) -> None:
        # Tests for ints
        state.set_config("test_int", 1)
        assert state.get_config("test_int") == 1
        state.set_config("test_int", 2)
        state.set_config("test_int", "3")
        assert state.get_config("test_int", 3)
        with pytest.raises(MismatchedTypeException):
            state.set_config("test_int", 0.1)
        with pytest.raises(MismatchedTypeException):
            state.set_config("test_int", "some_string")
        state.set_config("test_int", None)

        # Tests for floats
        state.set_config("test_float", 0.1)
        assert state.get_config("test_float") == 0.1
        state.set_config("test_float", 0.2)
        state.set_config("test_float", "0.3")
        assert state.get_config("test_float") == 0.3

        with pytest.raises(MismatchedTypeException):
            state.set_config("test_float", 1)
        with pytest.raises(MismatchedTypeException):
            state.set_config("test_float", "some_string")
        state.set_config("test_float", None)

        # Tests for strings
        state.set_config("test_str", "some_string")
        assert state.get_config("test_str") == "some_string"
        state.set_config("test_str", "some_other_string")
        with pytest.raises(MismatchedTypeException):
            state.set_config("test_str", 1)
        with pytest.raises(MismatchedTypeException):
            state.set_config("test_str", 0.1)
        state.set_config("test_str", None)

        # Tests with force option
        state.set_config("some_key", 1)
        state.set_config("some_key", 0.1, force=True)
        assert state.get_config("some_key") == 0.1
        state.set_config("some_key", "some_value", force=True)
        assert state.get_config("some_key") == "some_value"

    def test_memoize(self) -> None:
        @state.memoize(0.1)
        def rand() -> float:
            return random.random()

        assert rand() == rand()
        rand1 = rand()
        assert rand1 == rand()
        time.sleep(0.1)
        assert rand1 != rand()


def test_safe_dumps():
    assert safe_dumps(ChainMap({"a": 1}, {"b": 2}), sort_keys=True,) == safe_dumps(
        {"a": 1, "b": 2}, sort_keys=True,
    )
