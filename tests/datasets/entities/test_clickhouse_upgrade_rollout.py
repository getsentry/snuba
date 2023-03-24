from typing import Optional

import pytest

from snuba.datasets.entities.clickhouse_upgrade import Choice, Option, RolloutSelector
from snuba.state import set_config

TESTS = [
    pytest.param(
        "configreferrer",
        0.0,
        0.0,
        0.0,
        0.0,
        Choice(Option.ERRORS, None),
        id="Nothing is rolled out",
    ),
    pytest.param(
        "another_referrer",
        1.0,
        0.0,
        1.0,
        0.0,
        Choice(Option.ERRORS, None),
        id="Global not rolled out. Query referrer different",
    ),
    pytest.param(
        "configreferrer",
        0.0,
        0.0,
        None,
        1.0,
        Choice(Option.ERRORS, Option.ERRORS_V2),
        id="Execute both globally rolled out",
    ),
    pytest.param(
        "configreferrer",
        None,
        1.0,
        None,
        1.0,
        Choice(Option.ERRORS_V2, Option.ERRORS),
        id="Execute both globally rolled out. Trust V2",
    ),
    pytest.param(
        "configreferrer",
        1.0,
        1.0,
        0.0,
        0.0,
        Choice(Option.ERRORS_V2, None),
        id="Trust and execute only V2",
    ),
    pytest.param(
        "configreferrer",
        1.0,
        0.0,
        1.0,
        None,
        Choice(Option.ERRORS_V2, Option.ERRORS),
        id="Trust and execute V2 rolled out by referrer",
    ),
    pytest.param(
        "configreferrer",
        0.0,
        1.0,
        0.0,
        1.0,
        Choice(Option.ERRORS, None),
        id="Override by referrer global config.",
    ),
]


@pytest.mark.parametrize(
    "query_referrer, referrer_trust_config, global_trust_config, referrer_execute_config, global_execute_config, expceted",
    TESTS,
)
@pytest.mark.redis_db
def test_rollout_percentage(
    query_referrer: str,
    referrer_trust_config: Optional[float],
    global_trust_config: Optional[float],
    referrer_execute_config: Optional[float],
    global_execute_config: Optional[float],
    expceted: Choice,
) -> None:
    set_config(
        "rollout_upgraded_errors_trust_configreferrer",
        referrer_trust_config,
    )
    set_config("rollout_upgraded_errors_trust", global_trust_config)

    set_config(
        "rollout_upgraded_errors_execute_configreferrer",
        referrer_execute_config,
    )
    set_config("rollout_upgraded_errors_execute", global_execute_config)

    rollout_selector = RolloutSelector(Option.ERRORS, Option.ERRORS_V2, "errors")

    assert rollout_selector.choose(query_referrer) == expceted
