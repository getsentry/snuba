from typing import Optional

import pytest

from snuba import settings
from snuba.datasets.entities.clickhouse_upgrade import Choice, Option, RolloutSelector
from snuba.state import set_config

TESTS = [
    pytest.param(
        "configreferrer",
        0.0,
        0.0,
        0.0,
        0.0,
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
        1.0,
        0.0,
        0.0,
        1.0,
        1.0,
        0.0,
        0.0,
        Choice(Option.ERRORS, None),
        id="Global not rolled out. Query referrer different",
    ),
    pytest.param(
        "configreferrer",
        0.0,
        0.0,
        0.0,
        0.0,
        None,
        None,
        1.0,
        0.0,
        Choice(Option.ERRORS, Option.ERRORS_V2),
        id="Execute both globally rolled out",
    ),
    pytest.param(
        "configreferrer",
        0.0,
        0.0,
        0.0,
        0.0,
        None,
        None,
        None,
        1.0,
        Choice(Option.ERRORS, Option.ERRORS_V2),
        id="Execute both globally rolled out (setting)",
    ),
    pytest.param(
        "configreferrer",
        None,
        None,
        1.0,
        0.0,
        None,
        None,
        1.0,
        0.0,
        Choice(Option.ERRORS_V2, Option.ERRORS),
        id="Execute both globally rolled out. Trust V2",
    ),
    pytest.param(
        "configreferrer",
        None,
        None,
        None,
        1.0,
        None,
        None,
        None,
        1.0,
        Choice(Option.ERRORS_V2, Option.ERRORS),
        id="Execute both globally rolled out (setting). Trust V2",
    ),
    pytest.param(
        "configreferrer",
        None,
        None,
        None,
        1.0,
        None,
        None,
        None,
        0.0,
        Choice(Option.ERRORS_V2, None),
        id="Trust and execute only V2",
    ),
    pytest.param(
        "configreferrer",
        1.0,
        None,
        0.0,
        0.0,
        1.0,
        None,
        None,
        0.0,
        Choice(Option.ERRORS_V2, Option.ERRORS),
        id="Trust and execute V2 rolled out by referrer",
    ),
    pytest.param(
        "configreferrer",
        None,
        1.0,
        0.0,
        0.0,
        1.0,
        None,
        None,
        0.0,
        Choice(Option.ERRORS_V2, Option.ERRORS),
        id="Trust and execute V2 rolled out by referrer. Settings",
    ),
    pytest.param(
        "configreferrer",
        0.0,
        None,
        1.0,
        1.0,
        0.0,
        None,
        1.0,
        1.0,
        Choice(Option.ERRORS, None),
        id="Override by referrer global config.",
    ),
]


@pytest.mark.parametrize(
    "query_referrer, referrer_trust_config, referrer_trust_setting, global_trust_config, global_trust_setting, referrer_execute_config, referrer_execute_setting, global_execute_config, global_execute_setting, expceted",
    TESTS,
)
def test_rollout_percentage(
    query_referrer: str,
    referrer_trust_config: Optional[float],
    referrer_trust_setting: Optional[float],
    global_trust_config: Optional[float],
    global_trust_setting: float,
    referrer_execute_config: Optional[float],
    referrer_execute_setting: Optional[float],
    global_execute_config: Optional[float],
    global_execute_setting: float,
    expceted: Choice,
) -> None:
    set_config(
        "rollout_upgraded_errors_trust_configreferrer", referrer_trust_config,
    )
    if referrer_trust_setting is not None:
        settings.ERRORS_UPGRADE_TRUST_SECONDARY = {
            "configreferrer": referrer_trust_setting
        }
    set_config("rollout_upgraded_errors_trust", global_trust_config)
    settings.ERRORS_UPGRADE_TRUST_SECONDARY_GLOBAL = global_trust_setting

    set_config(
        "rollout_upgraded_errors_execute_configreferrer", referrer_execute_config,
    )
    if referrer_execute_setting is not None:
        settings.ERRORS_UPGRADE_EXECUTE_BOTH = {
            "configreferrer": referrer_execute_setting
        }
    set_config("rollout_upgraded_errors_execute", global_execute_config)
    settings.ERRORS_UPGRADE_EXECUTE_BOTH_GLOBAL = global_execute_setting

    rollout_selector = RolloutSelector(Option.ERRORS, Option.ERRORS_V2, "errors")

    assert rollout_selector.choose(query_referrer) == expceted

    # Reset settings
    settings.ERRORS_UPGRADE_TRUST_SECONDARY = {}
    settings.ERRORS_UPGRADE_EXECUTE_BOTH = {}
