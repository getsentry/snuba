import random
import time
from collections import ChainMap
from functools import partial

from snuba import state
from snuba.state import safe_dumps


class TestState:
    def setup_method(self):
        from snuba.web.views import application

        assert application.testing == True
        self.app = application.test_client()
        self.app.post = partial(self.app.post, headers={"referer": "test"})

    def test_config(self):
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

        state.set_configs({"bar": "quux"})
        all_configs = state.get_all_configs()
        assert all(
            all_configs[k] == v for k, v in [("foo", 1), ("bar", "quux"), ("baz", 3)]
        )

    def test_memoize(self):
        @state.memoize(0.1)
        def rand() -> float:
            return random.random()

        assert rand() == rand()
        rand1 = rand()
        assert rand1 == rand()
        time.sleep(0.1)
        assert rand1 != rand()

    def test_abtest(self):
        assert state.abtest("1000:1/2000:1") in (1000, 2000)
        assert state.abtest("1000/2000") in (1000, 2000)
        assert state.abtest("1000/2000:5") in (1000, 2000)
        assert state.abtest("1000/2000:0") == 1000
        assert state.abtest("1.5:1/-1.5:1") in (1.5, -1.5)


def test_safe_dumps():
    assert safe_dumps(ChainMap({"a": 1}, {"b": 2}), sort_keys=True,) == safe_dumps(
        {"a": 1, "b": 2}, sort_keys=True,
    )
