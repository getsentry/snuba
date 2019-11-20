from collections import ChainMap
from tests.base import BaseEventsTest
from functools import partial
import random
import simplejson as json
from threading import Thread
import time
import uuid

from snuba import state
from snuba.state import safe_dumps


class TestState(BaseEventsTest):
    def setup_method(self, test_method):
        super(TestState, self).setup_method(test_method)
        from snuba.views import application

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

    def test_dedupe(self):
        try:
            state.set_config("use_query_id", 1)
            state.set_config("use_cache", 1)
            uniq_name = uuid.uuid4().hex[:8]

            def do_request(result_container):
                result = json.loads(
                    self.app.post(
                        "/query",
                        data=json.dumps(
                            {
                                "project": 1,
                                "granularity": 3600,
                                "aggregations": [
                                    ["count()", "", uniq_name],
                                    ["sleep(0.01)", "", "sleep"],
                                ],
                            }
                        ),
                    ).data
                )
                result_container.append(result)

            # t0 and t1 are exact duplicate queries submitted concurrently.  One of
            # them will execute normally and the other one should be held back by
            # the deduper, until it can use the cached result from the first.
            results = [[] for _ in range(3)]
            t0 = Thread(target=do_request, args=(results[0],))
            t1 = Thread(target=do_request, args=(results[1],))
            t0.start()
            t1.start()
            t0.join()
            t1.join()

            # a subsequent request will not be marked as duplicate
            # as we waited for the first 2 to finish
            # it is still fresh
            do_request(results[2])

            results = [r.pop() for r in results]
            # The results should all have the same data
            datas = [r["data"] for r in results]
            assert datas[0] == [{uniq_name: 0, "sleep": 0}]
            assert all(d == datas[0] for d in datas)

            stats = [r["stats"] for r in results]
            # we don't know which order these will execute in, but one
            # of them will be a cached result
            assert stats[0]["cache_hit"] in (True, False)
            assert stats[1]["cache_hit"] in (True, False)
            assert stats[0]["cache_hit"] != stats[1]["cache_hit"]
            # and the cached one should be the one marked as dupe
            assert stats[0]["cache_hit"] == stats[0]["is_duplicate"]
            assert stats[1]["cache_hit"] == stats[1]["is_duplicate"]

            assert stats[2]["is_duplicate"] == False

        finally:
            state.delete_config("use_query_id")
            state.delete_config("use_cache")

    def test_memoize(self):
        @state.memoize(0.1)
        def rand():
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
