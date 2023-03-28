import random
import time
from collections import ChainMap
from functools import partial

import pytest

from snuba import state
from snuba.state import MismatchedTypeException, safe_dumps


class TestState:
    def setup_method(self) -> None:
        from snuba.web.views import application

        assert application.testing == True
        self.app = application.test_client()
        self.app.post = partial(self.app.post, headers={"referer": "test"})  # type: ignore

    @pytest.mark.redis_db
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

    @pytest.mark.redis_db
    def test_config_desc(self) -> None:
        state.set_config_description("foo", "Does foo")
        assert state.get_config_description("foo") == "Does foo"
        state.set_config_description("bar", "bars something")
        assert all(
            state.get_all_config_descriptions()[k] == d
            for k, d in [("foo", "Does foo"), ("bar", "bars something")]
        )
        state.delete_config_description("foo")
        assert state.get_config_description("foo") is None

    @pytest.mark.redis_db
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

    @pytest.mark.redis_db
    def test_memoize(self) -> None:
        @state.memoize(0.1)  # type: ignore
        def rand(config_key: str = "test") -> float:
            return random.random()

        assert rand() == rand()
        rand1 = rand()
        assert rand1 == rand()
        time.sleep(0.1)
        assert rand1 != rand()

    @pytest.mark.redis_db
    def test_memoize_with_args(self) -> None:
        @state.memoize(0.1)  # type: ignore
        def rand(config_key: str = "test1") -> str:
            return f"{random.random()}:{config_key}"

        assert rand() == rand()
        rand1 = rand()
        assert rand1 == rand()
        assert rand1 != rand("test2")
        time.sleep(0.1)
        assert rand1 != rand()

        assert rand("test2") == rand("test2")
        rand2 = rand("test2")
        time.sleep(0.1)
        assert rand2 != rand("test2")


def test_safe_dumps() -> None:
    assert safe_dumps(ChainMap({"a": 1}, {"b": 2}), sort_keys=True,) == safe_dumps(
        {"a": 1, "b": 2},
        sort_keys=True,
    )
