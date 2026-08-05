from snuba.web.rpc.storage_routing.routing_strategies.common import (
    num_items_from_outcomes_result,
)


def test_num_items_from_outcomes_result() -> None:
    assert num_items_from_outcomes_result({"data": [{"num_items": 42}]}) == 42
    assert num_items_from_outcomes_result({"data": []}) == 0
    assert num_items_from_outcomes_result({}) == 0
    assert num_items_from_outcomes_result({"data": [{"num_items": None}]}) == 0
